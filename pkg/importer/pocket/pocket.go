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
	"bufio"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"perkeep.org/internal/httputil"
	"perkeep.org/pkg/importer"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/schema/nodeattr"

	"github.com/garyburd/go-oauth/oauth"

	"go4.org/ctxutil"
	"go4.org/syncutil"
)

const (
	apiURL                        = "https://getpocket.com/v3"
	temporaryCredentialRequestURL = "https://getpocket.com/v3/oauth/request"
	resourceOwnerAuthorizationURL = "https://getpocket.com/auth/authorize"
	tokenRequestURL               = "https://getpocket.com/v3/oauth/authorize"
	userRetrievePath              = "get"

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

var oAuthURIs = importer.OAuthURIs{
	TemporaryCredentialRequestURI: temporaryCredentialRequestURL,
	ResourceOwnerAuthorizationURI: resourceOwnerAuthorizationURL,
	TokenRequestURI:               tokenRequestURL,
}

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

	oauthClient *oauth.Client      // No need to guard, used read-only.
	accessCreds *oauth.Credentials // No need to guard, used read-only.

	mu     sync.Mutex // guards anyErr
	anyErr bool
}

var forceFullImport, _ = strconv.ParseBool(os.Getenv("CAMLI_POCKET_FULL_IMPORT"))

func (im *imp) Run(ctx *importer.RunContext) error {
	clientId, secret, err := ctx.Credentials()
	if err != nil {
		return fmt.Errorf("no API credentials: %v", err)
	}
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

		oauthClient: &oauth.Client{
			TemporaryCredentialRequestURI: temporaryCredentialRequestURL,
			ResourceOwnerAuthorizationURI: resourceOwnerAuthorizationURL,
			TokenRequestURI:               tokenRequestURL,
			Credentials: oauth.Credentials{
				Token:  clientId,
				Secret: secret,
			},
		},
		accessCreds: &oauth.Credentials{
			Token:  accessToken,
			Secret: accessSecret,
		},
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

	acctNode, err = ctx.Host.ObjectFromRef(acctNode.PermanodeRef())
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

var _ importer.LongPoller = (*imp)(nil)

func (im *imp) LongPoll(rctx *importer.RunContext) error {
	clientId, secret, err := rctx.Credentials()
	if err != nil {
		return err
	}

	acctNode := rctx.AccountNode()
	accessToken := acctNode.Attr(importer.AcctAttrAccessToken)
	accessSecret := acctNode.Attr(importer.AcctAttrAccessTokenSecret)
	if accessToken == "" || accessSecret == "" {
		return errors.New("access credentials not found")
	}
	oauthClient := &oauth.Client{
		TemporaryCredentialRequestURI: temporaryCredentialRequestURL,
		ResourceOwnerAuthorizationURI: resourceOwnerAuthorizationURL,
		TokenRequestURI:               tokenRequestURL,
		Credentials: oauth.Credentials{
			Token:  clientId,
			Secret: secret,
		},
	}
	accessCreds := &oauth.Credentials{
		Token:  accessToken,
		Secret: accessSecret,
	}

	form := url.Values{"with": {"user"}}
	req, _ := http.NewRequest("GET", "https://userstream.twitter.com/1.1/user.json", nil)
	req.Header.Set("Authorization", oauthClient.AuthorizationHeader(accessCreds, "GET", req.URL, form))
	req.URL.RawQuery = form.Encode()
	req.Cancel = rctx.Context().Done()

	log.Printf("twitter: beginning long poll, awaiting new tweets...")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}
	bs := bufio.NewScanner(res.Body)
	for bs.Scan() {
		line := strings.TrimSpace(bs.Text())
		if line == "" || strings.HasPrefix(line, `{"friends`) {
			continue
		}
		log.Printf("twitter: long poll saw activity")
		return nil
	}
	if err := bs.Err(); err != nil {
		return err
	}
	return errors.New("twitter: got EOF without a tweet")
}

func (r *run) errorf(format string, args ...interface{}) {
	log.Printf("pocket: "+format, args...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.anyErr = true
}

func (r *run) doAPI(result interface{}, apiPath string, keyval ...string) error {
	return importer.OAuthContext{
		r.Context(),
		r.oauthClient,
		r.accessCreds}.PopulateJSONFromURL(result, http.MethodGet, apiURL+apiPath, keyval...)
}

// importItems imports the items related to userID, through apiPath.
func (r *run) importItems(userID string, apiPath string) error {
	maxID := ""
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

	// If attrs is changed, so should the expected responses accordingly for the
	// RoundTripper of MakeTestData (testdata.go).
	attrs := []string{
		"user_id", userID,
		"count", strconv.Itoa(tweetRequestLimit),
	}
	for continueRequests {
		select {
		case <-r.Context().Done():
			r.errorf("interrupted")
			return r.Context().Err()
		default:
		}

		var resp []*pocketItem
		var err error
		if maxID == "" {
			log.Printf("twitter: fetching %s for userid %s", importType, userID)
			err = r.doAPI(&resp, apiPath, attrs...)
		} else {
			log.Printf("twitter: fetching %s for userid %s with max ID %s", userID, importType, maxID)
			err = r.doAPI(&resp, apiPath,
				append(attrs, "max_id", maxID)...)
		}
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
			maxID = tweet.Id

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
					r.errorf("error importing tweet %s %v", tweet.Id, err)
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
	oauthClient, err := ctx.NewOAuthClient(oAuthURIs)
	if err != nil {
		err = fmt.Errorf("error getting OAuth client: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}
	oauthClient.SignForm()
	tempCred, err := oauthClient.RequestTemporaryCredentials(ctxutil.Client(ctx), ctx.CallbackURL(), nil)
	if err != nil {
		err = fmt.Errorf("Error getting temp cred: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}
	if err := ctx.AccountNode.SetAttrs(
		importer.AcctAttrTempToken, tempCred.Token,
		importer.AcctAttrTempSecret, tempCred.Secret,
	); err != nil {
		err = fmt.Errorf("Error saving temp creds: %v", err)
		httputil.ServeError(w, r, err)
		return err
	}

	authURL := oauthClient.AuthorizationURL(tempCred, nil)
	http.Redirect(w, r, authURL, 302)
	return nil
}

func (im *imp) ServeCallback(w http.ResponseWriter, r *http.Request, ctx *importer.SetupContext) {
	tempToken := ctx.AccountNode.Attr(importer.AcctAttrTempToken)
	tempSecret := ctx.AccountNode.Attr(importer.AcctAttrTempSecret)
	if tempToken == "" || tempSecret == "" {
		log.Printf("pocket: no temp creds in callback")
		httputil.BadRequestError(w, "no temp creds in callback")
		return
	}
	if tempToken != r.FormValue("oauth_token") {
		log.Printf("pocket: unexpected oauth_token: got %v, want %v", r.FormValue("oauth_token"), tempToken)
		httputil.BadRequestError(w, "unexpected oauth_token")
		return
	}
	oauthClient, err := ctx.NewOAuthClient(oAuthURIs)
	if err != nil {
		err = fmt.Errorf("error getting OAuth client: %v", err)
		httputil.ServeError(w, r, err)
		return
	}
	tokenCred, vals, err := oauthClient.RequestToken(
		ctxutil.Client(ctx),
		&oauth.Credentials{
			Token:  tempToken,
			Secret: tempSecret,
		},
		r.FormValue("oauth_verifier"),
	)
	if err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error getting request token: %v ", err))
		return
	}
	userid := vals.Get("user_id")
	if userid == "" {
		httputil.ServeError(w, r, fmt.Errorf("Couldn't get user id: %v", err))
		return
	}
	if err := ctx.AccountNode.SetAttrs(
		importer.AcctAttrAccessToken, tokenCred.Token,
		importer.AcctAttrAccessTokenSecret, tokenCred.Secret,
	); err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error setting token attributes: %v", err))
		return
	}

	if err := ctx.AccountNode.SetAttrs(
		nodeattr.Title, fmt.Sprintf("%s's Pocket Account", "TODO"),
	); err != nil {
		httputil.ServeError(w, r, fmt.Errorf("Error setting attribute: %v", err))
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
