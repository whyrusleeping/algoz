package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
)

type ImageClassifier struct {
	Host       string
	Categories []string
}

func (ic *ImageClassifier) Classify(ctx context.Context, img []byte) (string, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)

	// Add file to the request
	part, err := writer.CreateFormFile("upload", "image.jpg")
	if err != nil {
		return "", err
	}
	part.Write(img)

	err = writer.Close()
	if err != nil {
		return "", err
	}

	u, err := url.Parse(ic.Host + "/classify_image/")
	if err != nil {
		return "", err
	}

	q := u.Query()
	for _, c := range ic.Categories {
		q.Add("category", c)
	}
	u.RawQuery = q.Encode()

	// Create the request
	req, err := http.NewRequest("POST", u.String(), &buffer)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Send the request
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	// Print the response
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Unexpected status code %d", res.StatusCode)
	}

	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return "", err
	}

	c, ok := out["category"].(string)
	if !ok {
		return "", fmt.Errorf("no category in response")
	}
	return c, nil
}
