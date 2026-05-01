package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/httpclient"
)

type oauthClientJSON struct {
	Installed struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		AuthURI      string   `json:"auth_uri"`
		TokenURI     string   `json:"token_uri"`
		RedirectURIs []string `json:"redirect_uris"`
	} `json:"installed"`
}

const googleDriveScope = "https://www.googleapis.com/auth/drive"

type tokenCache struct {
	RefreshToken string `json:"refresh_token"`
}

type googleFile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type GoogleBackend struct {
	name            string
	apiClient       *http.Client
	tokenClient     *http.Client
	credentialsPath string
	folderID        string
	retryCfg        RetryConfig
	limiter         *requestRateLimiter

	clientID     string
	clientSecret string
	tokenURI     string
	authURI      string
	redirectURI  string

	token        string
	refreshToken string
	tokenEx      time.Time
	mu           sync.Mutex
	listAfterMs  int64

	fileIDs   map[string]string
	fileIDsMu sync.RWMutex

	healthMu   sync.RWMutex
	health     HealthState
	healthSink func(HealthState)
	verbose    bool
}

func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		name:            "google",
		apiClient:       client,
		tokenClient:     client,
		credentialsPath: saPath,
		folderID:        folderID,
		fileIDs:         make(map[string]string),
		health:          HealthHealthy,
		retryCfg: RetryConfig{
			MinBackoffMs:           500,
			MaxBackoffMs:           30000,
			BackoffMultiplier:      2,
			JitterPercent:          25,
			MaxRetriesPerOperation: 6,
		},
		verbose: googleVerboseFromEnv(),
	}
}

func NewGoogleBackendWithOptions(opts GoogleBackendOptions) *GoogleBackend {
	opts.APITransport.ApplyDefaults()
	opts.TokenTransport.ApplyDefaults()
	apiClient := httpclient.NewCustomClient(opts.APITransport)
	tokenClient := httpclient.NewCustomClient(opts.TokenTransport)
	return &GoogleBackend{
		name:            opts.Name,
		apiClient:       apiClient,
		tokenClient:     tokenClient,
		credentialsPath: opts.CredentialsPath,
		folderID:        opts.FolderID,
		tokenURI:        opts.TokenURL,
		fileIDs:         make(map[string]string),
		health:          HealthHealthy,
		retryCfg:        opts.Retry,
		limiter:         newRequestRateLimiter(opts.Name, opts.RateLimits),
		verbose:         googleVerboseFromEnv(),
	}
}

func (b *GoogleBackend) SetHealthSink(sink func(HealthState)) {
	b.healthMu.Lock()
	b.healthSink = sink
	b.healthMu.Unlock()
}

func (b *GoogleBackend) HealthState() HealthState {
	b.healthMu.RLock()
	defer b.healthMu.RUnlock()
	return b.health
}

func (b *GoogleBackend) setHealth(state HealthState) {
	b.healthMu.Lock()
	if b.health == state {
		b.healthMu.Unlock()
		return
	}
	old := b.health
	b.health = state
	sink := b.healthSink
	b.healthMu.Unlock()

	log.Printf("backend %s health state: %s -> %s", b.name, old, state)
	if sink != nil {
		sink(state)
	}
}

func (b *GoogleBackend) Login(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := os.ReadFile(b.credentialsPath)
	if err != nil {
		return fmt.Errorf("failed to read client credentials %s: %w", b.credentialsPath, err)
	}
	var oauthJSON oauthClientJSON
	if err := json.Unmarshal(data, &oauthJSON); err != nil {
		return fmt.Errorf("failed to decode credentials JSON: %w", err)
	}

	b.clientID = oauthJSON.Installed.ClientID
	b.clientSecret = oauthJSON.Installed.ClientSecret
	b.authURI = oauthJSON.Installed.AuthURI
	if b.authURI == "" {
		b.authURI = "https://accounts.google.com/o/oauth2/auth"
	}
	if b.tokenURI == "" {
		b.tokenURI = oauthJSON.Installed.TokenURI
	}
	if b.tokenURI == "" {
		b.tokenURI = "https://oauth2.googleapis.com/token"
	}
	if len(oauthJSON.Installed.RedirectURIs) > 0 {
		b.redirectURI = oauthJSON.Installed.RedirectURIs[0]
	} else {
		b.redirectURI = "http://localhost"
	}

	tokenCachePath := b.credentialsPath + ".token"
	if cacheData, err := os.ReadFile(tokenCachePath); err == nil {
		var cache tokenCache
		if err := json.Unmarshal(cacheData, &cache); err == nil && cache.RefreshToken != "" {
			b.refreshToken = cache.RefreshToken
			if err := b.refreshAccessTokenLocked(ctx); err == nil {
				b.setHealth(HealthHealthy)
				return nil
			}
		}
	}

	link := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=%s&access_type=offline",
		b.authURI, url.QueryEscape(b.clientID), url.QueryEscape(b.redirectURI), url.QueryEscape(googleDriveScope))

	fmt.Printf("\n==================== OAUTH AUTHENTICATION REQUIRED ====================\n")
	fmt.Printf("1. Please open this URL in your web browser:\n\n%s\n\n", link)
	fmt.Printf("2. Authenticate and accept the permissions.\n")
	fmt.Printf("3. Copy the full redirected URL or the authorization code and paste it below.\n")
	fmt.Printf("\nEnter URL or Code: ")

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	code := input
	if strings.HasPrefix(input, "http") {
		u, err := url.Parse(input)
		if err == nil {
			if qCode := u.Query().Get("code"); qCode != "" {
				code = qCode
			}
		}
	}
	if code == "" {
		return fmt.Errorf("invalid authorization code")
	}

	if err := b.exchangeCodeLocked(ctx, code); err != nil {
		b.setHealth(HealthAuthFailed)
		return err
	}

	cache := tokenCache{RefreshToken: b.refreshToken}
	cacheBytes, _ := json.MarshalIndent(cache, "", "  ")
	if err := os.WriteFile(tokenCachePath, cacheBytes, 0600); err != nil {
		log.Printf("warning: failed to save refresh token cache %s: %v", tokenCachePath, err)
	}

	b.setHealth(HealthHealthy)
	return nil
}

func (b *GoogleBackend) SetListCreatedAfter(unixMs int64) {
	if unixMs < 0 {
		unixMs = 0
	}
	b.mu.Lock()
	b.listAfterMs = unixMs
	b.mu.Unlock()
}

func (b *GoogleBackend) listCreatedAfter() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listAfterMs
}

func googleVerboseFromEnv() bool {
	raw, ok := os.LookupEnv("FLOWDRIVER_GOOGLE_VERBOSE")
	if !ok {
		return false
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parsed, err := strconv.ParseBool(raw)
	if err == nil {
		return parsed
	}
	switch strings.ToLower(raw) {
	case "on", "yes", "debug", "verbose":
		return true
	default:
		return false
	}
}

func (b *GoogleBackend) vlogf(format string, args ...any) {
	if b == nil || !b.verbose {
		return
	}
	log.Printf(format, args...)
}

func (b *GoogleBackend) exchangeCodeLocked(ctx context.Context, code string) error {
	values := url.Values{}
	values.Set("grant_type", "authorization_code")
	values.Set("code", code)
	values.Set("client_id", b.clientID)
	values.Set("client_secret", b.clientSecret)
	values.Set("redirect_uri", b.redirectURI)
	return b.executeTokenRequestLocked(ctx, values)
}

func (b *GoogleBackend) refreshAccessTokenLocked(ctx context.Context) error {
	values := url.Values{}
	values.Set("grant_type", "refresh_token")
	values.Set("refresh_token", b.refreshToken)
	values.Set("client_id", b.clientID)
	values.Set("client_secret", b.clientSecret)
	return b.executeTokenRequestLocked(ctx, values)
}

func (b *GoogleBackend) executeTokenRequestLocked(ctx context.Context, values url.Values) error {
	_, err := retryOperation(ctx, b.retryCfg, "token", false, func() (struct{}, error) {
		if err := b.limit(ctx, "token"); err != nil {
			return struct{}{}, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.tokenURI, strings.NewReader(values.Encode()))
		if err != nil {
			return struct{}{}, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := b.tokenClient.Do(req)
		if err != nil {
			return struct{}{}, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return struct{}{}, readHTTPError("token request", resp)
		}

		var resData struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int    `json:"expires_in"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
			return struct{}{}, fmt.Errorf("failed to decode token response: %w", err)
		}
		b.token = resData.AccessToken
		if resData.RefreshToken != "" {
			b.refreshToken = resData.RefreshToken
		}
		b.tokenEx = time.Now().Add(time.Duration(resData.ExpiresIn-60) * time.Second)
		return struct{}{}, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("token", attempt, err, delay)
	})
	if err != nil {
		if isUnauthorizedError(err) {
			b.setHealth(HealthAuthFailed)
		}
		return err
	}
	b.setHealth(HealthHealthy)
	return nil
}

func (b *GoogleBackend) getValidToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.token != "" && time.Now().Before(b.tokenEx) {
		return b.token, nil
	}
	if err := b.refreshAccessTokenLocked(ctx); err != nil {
		return "", err
	}
	return b.token, nil
}

func (b *GoogleBackend) ValidateFolder(ctx context.Context) error {
	if strings.TrimSpace(b.folderID) == "" {
		return nil
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}
	if err := b.limit(ctx, "folder validate"); err != nil {
		return err
	}

	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files/" + url.PathEscape(b.folderID))
	values := u.Query()
	values.Set("fields", "id,name,mimeType,trashed")
	values.Set("supportsAllDrives", "true")
	u.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.apiClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return b.wrapFolderAccessError(readHTTPError("validate folder", resp))
	}

	var res struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		MimeType string `json:"mimeType"`
		Trashed  bool   `json:"trashed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return fmt.Errorf("failed to decode folder metadata for %q: %w", b.folderID, err)
	}
	if res.Trashed {
		return fmt.Errorf("configured folder_id %q points to a trashed Drive item", b.folderID)
	}
	if res.MimeType != "application/vnd.google-apps.folder" {
		return fmt.Errorf("configured folder_id %q points to %q, not a Drive folder", b.folderID, res.Name)
	}
	return nil
}

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	started := time.Now()
	attempts := 0
	payload, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to buffer upload payload: %w", err)
	}
	if err := b.reserveUpload(int64(len(payload))); err != nil {
		return err
	}

	_, err = retryOperation(ctx, b.retryCfg, "upload", false, func() (struct{}, error) {
		attempts++
		tok, err := b.getValidToken(ctx)
		if err != nil {
			return struct{}{}, err
		}
		if err := b.limit(ctx, "upload"); err != nil {
			return struct{}{}, err
		}
		contentType, body, err := b.multipartBody(filename, payload, true)
		if err != nil {
			return struct{}{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", bytes.NewReader(body))
		if err != nil {
			return struct{}{}, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", contentType)

		resp, err := b.apiClient.Do(req)
		if err != nil {
			return struct{}{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return struct{}{}, readHTTPError("upload", resp)
		}

		var res struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil && err != io.EOF {
			return struct{}{}, err
		}
		if res.ID != "" {
			b.cacheFileID(filename, res.ID)
		}
		return struct{}{}, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("upload", attempt, err, delay)
	})
	if err == nil {
		b.setHealth(HealthHealthy)
	}
	b.vlogf("backend %s op=upload filename=%s bytes=%d attempts=%d elapsed=%s err=%v",
		b.name, filename, len(payload), attempts, time.Since(started), err)
	return err
}

func (b *GoogleBackend) Put(ctx context.Context, filename string, data io.Reader) error {
	started := time.Now()
	attempts := 0
	payload, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to buffer put payload: %w", err)
	}
	if err := b.reserveUpload(int64(len(payload))); err != nil {
		return err
	}

	_, err = retryOperation(ctx, b.retryCfg, "put", false, func() (struct{}, error) {
		attempts++
		tok, err := b.getValidToken(ctx)
		if err != nil {
			return struct{}{}, err
		}
		if err := b.limit(ctx, "put"); err != nil {
			return struct{}{}, err
		}

		existing, _ := b.lookupExactName(ctx, filename)
		method := http.MethodPost
		urlStr := "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
		includeParents := true
		if len(existing) > 0 {
			method = http.MethodPatch
			urlStr = "https://www.googleapis.com/upload/drive/v3/files/" + existing[0].ID + "?uploadType=multipart"
			includeParents = false
		}

		contentType, body, err := b.multipartBody(filename, payload, includeParents)
		if err != nil {
			return struct{}{}, err
		}

		req, err := http.NewRequestWithContext(ctx, method, urlStr, bytes.NewReader(body))
		if err != nil {
			return struct{}{}, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", contentType)

		resp, err := b.apiClient.Do(req)
		if err != nil {
			return struct{}{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return struct{}{}, readHTTPError("put", resp)
		}

		var res struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil && err != io.EOF {
			return struct{}{}, err
		}
		if res.ID != "" {
			b.cacheFileID(filename, res.ID)
		}
		return struct{}{}, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("put", attempt, err, delay)
	})
	if err == nil {
		b.setHealth(HealthHealthy)
	}
	b.vlogf("backend %s op=put filename=%s bytes=%d attempts=%d elapsed=%s err=%v",
		b.name, filename, len(payload), attempts, time.Since(started), err)
	return err
}

func (b *GoogleBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	started := time.Now()
	files, err := b.listFiles(ctx, prefix)
	if err != nil {
		b.vlogf("backend %s op=list_query prefix=%q elapsed=%s err=%v", b.name, prefix, time.Since(started), err)
		return nil, err
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		b.cacheFileID(f.Name, f.ID)
		if prefix == "" || strings.HasPrefix(f.Name, prefix) {
			names = append(names, f.Name)
		}
	}
	sort.Strings(names)
	b.setHealth(HealthHealthy)
	b.vlogf("backend %s op=list_query prefix=%q names=%d elapsed=%s", b.name, prefix, len(names), time.Since(started))
	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	started := time.Now()
	attempts := 0
	contentLength := int64(-1)
	rc, err := retryOperation(ctx, b.retryCfg, "download", false, func() (io.ReadCloser, error) {
		attempts++
		fileID, err := b.fileIDForName(ctx, filename)
		if err != nil {
			return nil, err
		}
		tok, err := b.getValidToken(ctx)
		if err != nil {
			return nil, err
		}
		if err := b.limit(ctx, "download"); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.googleapis.com/drive/v3/files/"+fileID+"?alt=media", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)

		resp, err := b.apiClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				b.evictFileID(filename)
			}
			return nil, readHTTPError("download", resp)
		}

		contentLength = resp.ContentLength
		if err := b.reserveDownload(resp.ContentLength); err != nil {
			resp.Body.Close()
			return nil, err
		}
		return resp.Body, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("download", attempt, err, delay)
	})
	if err == nil {
		b.setHealth(HealthHealthy)
	}
	b.vlogf("backend %s op=download filename=%s content_length=%d attempts=%d elapsed=%s err=%v",
		b.name, filename, contentLength, attempts, time.Since(started), err)
	return rc, err
}

func (b *GoogleBackend) Delete(ctx context.Context, filename string) error {
	b.fileIDsMu.RLock()
	cachedID := b.fileIDs[filename]
	b.fileIDsMu.RUnlock()
	if cachedID != "" {
		if err := b.deleteFileByID(ctx, filename, cachedID); err == nil {
			b.evictFileID(filename)
			b.setHealth(HealthHealthy)
			return nil
		} else if !IsNotFoundError(err) {
			return err
		}
	}

	files, err := b.lookupExactName(ctx, filename)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		b.evictFileID(filename)
		return nil
	}

	for _, f := range files {
		if err := b.deleteFileByID(ctx, filename, f.ID); err != nil {
			return err
		}
	}

	b.evictFileID(filename)
	b.setHealth(HealthHealthy)
	return nil
}

func (b *GoogleBackend) deleteFileByID(ctx context.Context, filename, fileID string) error {
	_, err := retryOperation(ctx, b.retryCfg, "delete", false, func() (struct{}, error) {
		tok, err := b.getValidToken(ctx)
		if err != nil {
			return struct{}{}, err
		}
		if err := b.limit(ctx, "delete"); err != nil {
			return struct{}{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "https://www.googleapis.com/drive/v3/files/"+fileID, nil)
		if err != nil {
			return struct{}{}, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)

		resp, err := b.apiClient.Do(req)
		if err != nil {
			return struct{}{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
			return struct{}{}, readHTTPError("delete", resp)
		}
		return struct{}{}, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("delete", attempt, err, delay)
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *GoogleBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	meta := map[string]any{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
	}
	body, _ := json.Marshal(meta)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://www.googleapis.com/drive/v3/files", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.apiClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", readHTTPError("create folder", resp)
	}

	var res struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	b.folderID = res.ID
	return res.ID, nil
}

func (b *GoogleBackend) FindFolder(ctx context.Context, name string) (string, error) {
	q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", escapeDriveQuery(name))
	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	values := u.Query()
	values.Set("q", q)
	values.Set("fields", "nextPageToken,files(id,name)")
	values.Set("includeItemsFromAllDrives", "true")
	values.Set("supportsAllDrives", "true")
	u.RawQuery = values.Encode()

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.apiClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", readHTTPError("find folder", resp)
	}

	var res struct {
		Files []googleFile `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	if len(res.Files) == 0 {
		return "", nil
	}
	b.folderID = res.Files[0].ID
	return res.Files[0].ID, nil
}

func (b *GoogleBackend) multipartBody(filename string, payload []byte, includeParents bool) (string, []byte, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	header := make(textproto.MIMEHeader)
	header.Set("Content-Type", "application/json; charset=UTF-8")
	part, err := writer.CreatePart(header)
	if err != nil {
		return "", nil, err
	}

	meta := map[string]any{"name": filename}
	if includeParents && b.folderID != "" {
		meta["parents"] = []string{b.folderID}
	}
	if err := json.NewEncoder(part).Encode(meta); err != nil {
		return "", nil, err
	}

	header = make(textproto.MIMEHeader)
	header.Set("Content-Type", "application/octet-stream")
	part, err = writer.CreatePart(header)
	if err != nil {
		return "", nil, err
	}
	if _, err := part.Write(payload); err != nil {
		return "", nil, err
	}
	if err := writer.Close(); err != nil {
		return "", nil, err
	}
	return writer.FormDataContentType(), body.Bytes(), nil
}

func (b *GoogleBackend) listFiles(ctx context.Context, prefix string) ([]googleFile, error) {
	query := fmt.Sprintf("name contains '%s' and trashed = false", escapeDriveQuery(prefix))
	if b.folderID != "" {
		query += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}
	if createdAfter := b.listCreatedAfter(); createdAfter > 0 {
		query += fmt.Sprintf(" and createdTime > '%s'", time.UnixMilli(createdAfter).UTC().Format(time.RFC3339))
	}
	return b.queryFiles(ctx, query)
}

func (b *GoogleBackend) lookupExactName(ctx context.Context, filename string) ([]googleFile, error) {
	query := fmt.Sprintf("name = '%s' and trashed = false", escapeDriveQuery(filename))
	if b.folderID != "" {
		query += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}
	files, err := b.queryFiles(ctx, query)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		b.cacheFileID(f.Name, f.ID)
	}
	return files, nil
}

func (b *GoogleBackend) queryFiles(ctx context.Context, query string) ([]googleFile, error) {
	started := time.Now()
	attempts := 0
	pages := 0
	files, err := retryOperation(ctx, b.retryCfg, "list", false, func() ([]googleFile, error) {
		attempts++
		tok, err := b.getValidToken(ctx)
		if err != nil {
			return nil, err
		}
		if err := b.limit(ctx, "list"); err != nil {
			return nil, err
		}

		var all []googleFile
		pageToken := ""
		for {
			pages++
			u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
			values := u.Query()
			values.Set("q", query)
			values.Set("fields", "nextPageToken,files(id,name)")
			values.Set("pageSize", "1000")
			values.Set("includeItemsFromAllDrives", "true")
			values.Set("supportsAllDrives", "true")
			if pageToken != "" {
				values.Set("pageToken", pageToken)
			}
			u.RawQuery = values.Encode()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+tok)

			resp, err := b.apiClient.Do(req)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK {
				defer resp.Body.Close()
				return nil, readHTTPError("list", resp)
			}

			var res struct {
				NextPageToken string       `json:"nextPageToken"`
				Files         []googleFile `json:"files"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				resp.Body.Close()
				return nil, err
			}
			resp.Body.Close()
			all = append(all, res.Files...)
			if res.NextPageToken == "" {
				break
			}
			pageToken = res.NextPageToken
		}
		return all, nil
	}, func(attempt int, err error, delay time.Duration) {
		b.onRetry("list", attempt, err, delay)
	})
	b.vlogf("backend %s op=list attempts=%d pages=%d elapsed=%s query=%q err=%v",
		b.name, attempts, pages, time.Since(started), query, err)
	return files, err
}

func (b *GoogleBackend) fileIDForName(ctx context.Context, filename string) (string, error) {
	b.fileIDsMu.RLock()
	fileID := b.fileIDs[filename]
	b.fileIDsMu.RUnlock()
	if fileID != "" {
		return fileID, nil
	}

	files, err := b.lookupExactName(ctx, filename)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fmt.Errorf("file not found: %s", filename)
	}
	return files[0].ID, nil
}

func (b *GoogleBackend) cacheFileID(filename, fileID string) {
	b.fileIDsMu.Lock()
	if len(b.fileIDs) > 8192 {
		b.fileIDs = make(map[string]string)
	}
	b.fileIDs[filename] = fileID
	b.fileIDsMu.Unlock()
}

func (b *GoogleBackend) evictFileID(filename string) {
	b.fileIDsMu.Lock()
	delete(b.fileIDs, filename)
	b.fileIDsMu.Unlock()
}

func (b *GoogleBackend) limit(ctx context.Context, op string) error {
	if b.limiter == nil {
		return nil
	}
	return b.limiter.wait(ctx, op)
}

func (b *GoogleBackend) reserveUpload(bytes int64) error {
	if b.limiter == nil {
		return nil
	}
	if err := b.limiter.reserveUpload(bytes); err != nil {
		b.setHealth(HealthRateLimited)
		log.Printf("backend %s [RATE_LIMIT] reserve upload bytes=%d: %v", b.name, bytes, err)
		return err
	}
	return nil
}

func (b *GoogleBackend) reserveDownload(bytes int64) error {
	if b.limiter == nil {
		return nil
	}
	if err := b.limiter.reserveDownload(bytes); err != nil {
		b.setHealth(HealthRateLimited)
		log.Printf("backend %s [RATE_LIMIT] reserve download bytes=%d: %v", b.name, bytes, err)
		return err
	}
	return nil
}

func (b *GoogleBackend) onRetry(op string, attempt int, err error, delay time.Duration) {
	log.Printf("backend %s retry %s attempt %d in %s: %v", b.name, op, attempt, delay, err)
	if isUnauthorizedError(err) {
		b.setHealth(HealthAuthFailed)
		return
	}
	if isRateLimitedError(err) {
		log.Printf("backend %s [RATE_LIMIT] op=%s attempt=%d retry_in=%s err=%v", b.name, op, attempt, delay, err)
		b.setHealth(HealthRateLimited)
		return
	}
	if isRetryableError(err) {
		b.setHealth(HealthDegraded)
	}
}

func (b *GoogleBackend) wrapFolderAccessError(err error) error {
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) {
		return err
	}

	switch statusErr.StatusCode {
	case http.StatusForbidden, http.StatusNotFound:
		tokenCachePath := b.credentialsPath + ".token"
		return fmt.Errorf("configured folder_id %q is not accessible to this OAuth client/token; if the folder was recreated or moved under a different Google app/account, delete %s and authenticate again: %w", b.folderID, tokenCachePath, err)
	default:
		return err
	}
}

func readHTTPError(op string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return &httpStatusError{
		Operation:  op,
		StatusCode: resp.StatusCode,
		Body:       string(body),
	}
}

func escapeDriveQuery(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func retryOperation[T any](ctx context.Context, cfg RetryConfig, op string, retryForever bool, fn func() (T, error), onRetry func(int, error, time.Duration)) (T, error) {
	var zero T
	attempt := 0
	for {
		value, err := fn()
		if err == nil {
			return value, nil
		}

		if isUnauthorizedError(err) {
			return zero, err
		}
		if !isRetryableError(err) {
			return zero, err
		}

		attempt++
		if !retryForever && attempt > cfg.MaxRetriesPerOperation {
			return zero, err
		}
		delay := retryDelay(cfg, attempt)
		if onRetry != nil {
			onRetry(attempt, err, delay)
		}
		if err := waitForRetry(ctx, delay); err != nil {
			return zero, err
		}
	}
}
