/*
Copyright 2019 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pocket implements a getpocket.com importer.
package pocket // import "perkeep.org/pkg/importer/pocket"

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"perkeep.org/internal/httputil"
	"perkeep.org/pkg/importer"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/schema/nodeattr"

	"go4.org/ctxutil"
	"go4.org/syncutil"
)

const (
	apiURL           = "https://getpocket.com/v3"
	tokenURL         = "https://getpocket.com/v3/oauth/request"
	authURL          = "https://getpocket.com/v3/oauth/authorize"
	userRetrievePath = "get"

	// runCompleteVersion is a cache-busting version number of the
	// importer code. It should be incremented whenever the
	// behavior of this importer is updated enough to warrant a
	// complete run.  Otherwise, if the importer runs to
	// completion, this version number is recorded on the account
	// permanode and subsequent importers can stop early.
	runCompleteVersion = "1"

	tweetRequestLimit = 200 // max number of tweets we can get in a user_timeline request
	tweetsAtOnce      = 20  // how many tweets to import at once

	// A tweet is stored as a permanode with the "twitter.com:tweet" camliNodeType value.
	nodeTypeArticle = "getpocket.com:article"
)

func init() {
	importer.Register("pocket", &imp{})
}

var _ importer.ImporterSetupHTMLer = (*imp)(nil)

type imp struct {
	importer.OAuth2 // for CallbackRequestAccount and CallbackURLParameters
}

func (*imp) Properties() importer.Properties {
	return importer.Properties{
		Title:               "Pocket",
		Description:         "import articles",
		SupportsIncremental: true,
		NeedsAPIKey:         true,
	}
}

func (im *imp) IsAccountReady(acctNode *importer.Object) (ok bool, err error) {
	if acctNode.Attr(importer.AcctAttrUserID) != "" && acctNode.Attr(importer.AcctAttrAccessToken) != "" {
		return true, nil
	}
	return false, nil
}

func (im *imp) SummarizeAccount(acct *importer.Object) string {
	ok, err := im.IsAccountReady(acct)
	if err != nil {
		return "Not configured; error = " + err.Error()
	}
	if !ok {
		return "Not configured"
	}
	s := fmt.Sprintf("@%s (%s), pocket id %s",
		acct.Attr(importer.AcctAttrUserName),
		acct.Attr(importer.AcctAttrName),
		acct.Attr(importer.AcctAttrUserID),
	)
	return s
}

func (im *imp) AccountSetupHTML(host *importer.Host) string {
	base := host.ImporterBaseURL() + "pocket"
	return fmt.Sprintf(`
<h1>Configuring Pocket</h1>
<p>Visit <a href='https://getpocket.com/developer/apps/new'>https://getpocket.com/developer/apps/new</a> and click create a new app.</p>
<p>Use the following settings:</p>
<ul>
  <li>Name: Does not matter. (perkeeper-importer).</li>
  <li>Description: Does not matter. (imports pocket data into camlistore).</li>
  <li>URL: <b>%s</b></li>
</ul>
`, base)
}

// A run is our state for a given run of the importer.
type run struct {
	*importer.RunContext
	im          *imp
	incremental bool // whether we've completed a run in the past

	mu     sync.Mutex // guards anyErr
	anyErr bool
}

var forceFullImport, _ = strconv.ParseBool(os.Getenv("CAMLI_POCKET_FULL_IMPORT"))

// auth returns a new oauth2 Config
func auth(ctx *importer.SetupContext) (*oauth2.Config, error) {
	clientID, secret, err := ctx.Credentials()
	if err != nil {
		return nil, err
	}
	return &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: secret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  authURL,
			TokenURL: tokenURL,
		},
		RedirectURL: ctx.CallbackURL(),
		// No scope needed for foursquare as far as I can tell
	}, nil
}

func (im *imp) Run(ctx *importer.RunContext) error {
	acctNode := ctx.AccountNode()
	accessToken := acctNode.Attr(importer.AcctAttrAccessToken)
	accessSecret := acctNode.Attr(importer.AcctAttrAccessTokenSecret)
	if accessToken == "" || accessSecret == "" {
		return errors.New("access credentials not found")
	}
	r := &run{
		RunContext:  ctx,
		im:          im,
		incremental: !forceFullImport && acctNode.Attr(importer.AcctAttrCompletedVersion) == runCompleteVersion,
	}

	userID := acctNode.Attr(importer.AcctAttrUserID)
	if userID == "" {
		return errors.New("userID hasn't been set by account setup")
	}

	skipAPIImport, _ := strconv.ParseBool(os.Getenv("CAMLI_POCKET_SKIP_API_INPORT"))
	if !skipAPIImport {
		if err := r.importItems(userID, userRetrievePath); err != nil {
			return err
		}
	}

	acctNode, err := ctx.Host.ObjectFromRef(acctNode.PermanodeRef())
	if err != nil {
		return fmt.Errorf("error reloading account node: %v", err)
	}

	r.mu.Lock()
	anyErr := r.anyErr
	r.mu.Unlock()

	if !anyErr {
		if err := acctNode.SetAttrs(importer.AcctAttrCompletedVersion, runCompleteVersion); err != nil {
			return err
		}
	}

	return nil
}

func (r *run) errorf(format string, args ...interface{}) {
	log.Printf("pocket: "+format, args...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.anyErr = true
}

func (r *run) doAPI(ctx context.Context, form url.Values, result interface{}, apiPath string, keyval ...string) error {
	if len(keyval)%2 == 1 {
		panic("Incorrect number of keyval arguments")
	}

	for i := 0; i < len(keyval); i += 2 {
		form.Set(keyval[i], keyval[i+1])
	}

	fullURL := apiURL + apiPath
	res, err := doGet(ctx, fullURL, form)
	if err != nil {
		return err
	}
	err = httputil.DecodeJSON(res, result)
	if err != nil {
		log.Printf("Error parsing response for %s: %v", fullURL, err)
	}
	return err
}

func doGet(ctx context.Context, url string, form url.Values) (*http.Response, error) {
	requestURL := url + "?" + form.Encode()
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := ctxutil.Client(ctx).Do(req)
	if err != nil {
		log.Printf("Error fetching %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Get request on %s failed with: %s", requestURL, res.Status)
	}
	return res, nil
}

// importItems imports the items related to userID, through apiPath.
func (r *run) importItems(userID string, apiPath string) error {
	continueRequests := true

	var itemNode *importer.Object
	var err error
	var importType = "articles"
	itemNode, err = r.getTopLevelNode(importType)
	if err != nil {
		return err
	}

	numTweets := 0
	sawTweet := map[string]bool{}

	for continueRequests {
		select {
		case <-r.Context().Done():
			r.errorf("interrupted")
			return r.Context().Err()
		default:
		}

		var resp []*pocketItem
		var err error
		log.Printf("pocket: fetching %s for userid %s", importType, userID)
		// resp, err = doGet(r.Context(), apiPath, nil)
		if err != nil {
			return err
		}

		var (
			newThisBatch = 0
			allDupMu     sync.Mutex
			allDups      = true
			gate         = syncutil.NewGate(tweetsAtOnce)
			grp          syncutil.Group
		)
		for i := range resp {
			tweet := resp[i]

			// Dup-suppression.
			if sawTweet[tweet.Id] {
				continue
			}
			sawTweet[tweet.Id] = true
			newThisBatch++

			gate.Start()
			grp.Go(func() error {
				defer gate.Done()
				dup, err := r.importTweet(itemNode, *tweet)
				if !dup {
					allDupMu.Lock()
					allDups = false
					allDupMu.Unlock()
				}
				if err != nil {
					r.errorf("error importing item %s %v", tweet.Id, err)
				}
				return err
			})
		}
		if err := grp.Err(); err != nil {
			return err
		}
		numTweets += newThisBatch
		log.Printf("twitter: imported %d %s this batch; %d total.", newThisBatch, importType, numTweets)
		if r.incremental && allDups {
			log.Printf("twitter: incremental import found end batch")
			break
		}
		continueRequests = newThisBatch > 0
	}
	log.Printf("twitter: successfully did full run of importing %d %s", numTweets, importType)
	return nil
}

func timeParseFirstFormat(timeStr string, format ...string) (t time.Time, err error) {
	if len(format) == 0 {
		panic("need more than 1 format")
	}
	for _, f := range format {
		t, err = time.Parse(f, timeStr)
		if err == nil {
			break
		}
	}
	return
}

// viaAPI is true if it came via the REST API, or false if it came via a zip file.
func (r *run) importTweet(parent *importer.Object, tweet pocketItem) (dup bool, err error) {
	select {
	case <-r.Context().Done():
		r.errorf("Twitter importer: interrupted")
		return false, r.Context().Err()
	default:
	}
	id := tweet.ID()
	tweetNode, err := parent.ChildPathObject(id)
	if err != nil {
		return false, err
	}

	// e.g. "2014-06-12 19:11:51 +0000"
	createdTime, err := timeParseFirstFormat(tweet.CreatedAt(), time.RubyDate, "2006-01-02 15:04:05 -0700")
	if err != nil {
		return false, fmt.Errorf("could not parse time %q: %v", tweet.CreatedAt(), err)
	}

	url := fmt.Sprintf("https://twitter.com/%s/status/%v",
		r.AccountNode().Attr(importer.AcctAttrUserName),
		id)

	nodeType := nodeTypeArticle

	attrs := []string{
		"twitterId", id,
		nodeattr.Type, nodeType,
		nodeattr.StartDate, schema.RFC3339FromTime(createdTime),
		nodeattr.Content, tweet.Text(),
		nodeattr.URL, url,
	}

	for i, m := range tweet.Media() {
		filename := m.BaseFilename()
		if tweetNode.Attr("camliPath:"+filename) != "" && (i > 0 || tweetNode.Attr("camliContentImage") != "") {
			// Don't re-import media we've already fetched.
			continue
		}
		tried, gotMedia := 0, false
		for _, mediaURL := range m.URLs() {
			tried++
			res, err := ctxutil.Client(r.Context()).Get(mediaURL)
			if err != nil {
				return false, fmt.Errorf("Error fetching %s for tweet %s : %v", mediaURL, url, err)
			}
			if res.StatusCode == http.StatusNotFound {
				continue
			}
			if res.StatusCode != 200 {
				return false, fmt.Errorf("HTTP status %d fetching %s for tweet %s", res.StatusCode, mediaURL, url)
			}
			fileRef, err := schema.WriteFileFromReader(r.Context(), r.Host.Target(), filename, res.Body)
			res.Body.Close()
			if err != nil {
				return false, fmt.Errorf("Error fetching media %s for tweet %s: %v", mediaURL, url, err)
			}
			attrs = append(attrs, "camliPath:"+filename, fileRef.String())
			if i == 0 {
				attrs = append(attrs, "camliContentImage", fileRef.String())
			}
			log.Printf("twitter: slurped %s as %s for tweet %s (%v)", mediaURL, fileRef.String(), url, tweetNode.PermanodeRef())
			gotMedia = true
			break
		}
		if !gotMedia && tried > 0 {
			return false, fmt.Errorf("All media URLs 404s for tweet %s", url)
		}
	}

	changes, err := tweetNode.SetAttrs2(attrs...)
	if err == nil && changes {
		log.Printf("twitter: imported tweet %s", url)
	}
	return !changes, err
}

// path may be of: "articles".
func (r *run) getTopLevelNode(path string) (*importer.Object, error) {
	acctNode := r.AccountNode()

	root := r.RootNode()
	rootTitle := fmt.Sprintf("%s's Pocket Data", acctNode.Attr(importer.AcctAttrUserName))
	if err := root.SetAttr(nodeattr.Title, rootTitle); err != nil {
		return nil, err
	}

	obj, err := root.ChildPathObject(path)
	if err != nil {
		return nil, err
	}
	var title string
	switch path {
	case "articles":
		title = fmt.Sprintf("%s's Articles", acctNode.Attr(importer.AcctAttrUserName))
	}
	return obj, obj.SetAttr(nodeattr.Title, title)
}

type userInfo struct {
	ID         string `json:"id_str"`
	ScreenName string `json:"screen_name"`
	Name       string `json:"name,omitempty"`
}

func (im *imp) ServeSetup(w http.ResponseWriter, r *http.Request, ctx *importer.SetupContext) error {
	oauthConfig, err := auth(ctx)
	if err != nil {
		return err
	}
	oauthConfig.RedirectURL = im.RedirectURL(im, ctx)
	state, err := im.RedirectState(im, ctx)
	if err != nil {
		return err
	}
	http.Redirect(w, r, oauthConfig.AuthCodeURL(state), http.StatusFound)
	return nil
}

func (im *imp) ServeCallback(w http.ResponseWriter, r *http.Request, ctx *importer.SetupContext) {
	oauthConfig, err := auth(ctx)
	if err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error getting oauth config: %v", err))
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Expected a GET", 400)
		return
	}
	code := r.FormValue("code")
	if code == "" {
		http.Error(w, "Expected a code", 400)
		return
	}
	token, err := oauthConfig.Exchange(ctx, code)
	log.Printf("Token = %#v, error %v", token, err)
	if err != nil {
		log.Printf("Token Exchange error: %v", err)
		http.Error(w, "token exchange error", 500)
		return
	}

	http.Redirect(w, r, ctx.AccountURL(), http.StatusFound)

}

type pocketMedia interface {
	URLs() []string // use first non-404 one
	BaseFilename() string
}

type pocketItem struct {
	Id           string   `json:"item_id"`
	TextStr      string   `json:"text"`
	CreatedAtStr string   `json:"created_at"`
	Entities     entities `json:"entities"`
	Favorited    bool     `json:"favorited"`
}

func (t *pocketItem) ID() string {
	if t.Id == "" {
		panic("empty id")
	}
	return t.Id
}

func (t *pocketItem) CreatedAt() string { return t.CreatedAtStr }

func (t *pocketItem) Text() string { return html.UnescapeString(t.TextStr) }

func (t *pocketItem) Media() (ret []pocketMedia) {
	for _, m := range t.Entities.Media {
		ret = append(ret, m)
	}
	ret = append(ret, getImagesFromURLs(t.Entities.URLs)...)
	return
}

type entities struct {
	Media []*media     `json:"media"`
	URLs  []*urlEntity `json:"urls"`
}

type urlEntity struct {
	URL         string `json:"url"`
	ExpandedURL string `json:"expanded_url"`
	DisplayURL  string `json:"display_url"`
}

func getImagesFromURLs(urls []*urlEntity) (ret []pocketMedia) {
	//for _, u := range urls {
	// TODO get images
	//}
	return
}

// The Media entity from the Rest API. See also: zipMedia.
type media struct {
	Id            string               `json:"id_str"`
	IdNum         int64                `json:"id"`
	MediaURL      string               `json:"media_url"`
	MediaURLHTTPS string               `json:"media_url_https"`
	Sizes         map[string]mediaSize `json:"sizes"`
	Type_         string               `json:"type"`
}

func (m *media) URLs() []string {
	u := m.baseURL()
	if u == "" {
		return nil
	}
	return []string{u + m.largestMediaSuffix(), u}
}

func (m *media) baseURL() string {
	if v := m.MediaURLHTTPS; v != "" {
		return v
	}
	return m.MediaURL
}

func (m *media) BaseFilename() string {
	return path.Base(m.baseURL())
}

func (m *media) largestMediaSuffix() string {
	bestPixels := 0
	bestSuffix := ""
	for k, sz := range m.Sizes {
		if px := sz.W * sz.H; px > bestPixels {
			bestPixels = px
			bestSuffix = ":" + k
		}
	}
	return bestSuffix
}

type mediaSize struct {
	W      int    `json:"w"`
	H      int    `json:"h"`
	Resize string `json:"resize"`
}
