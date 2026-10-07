// Command sqssign signs a wager envelope the way the trusted gateway must:
// it reads the envelope JSON from stdin, signs it with the key in
// SQS_SIGNING_KEY (the key of the envelope's providerId) and prints the signed
// envelope. It stands in for the gateway in local examples and is a reference
// for gateway implementations; it does not authenticate the provider itself.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"wagering/internal/sqsauth"
	"wagering/internal/sqsconsumer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sqssign:", err)
		os.Exit(1)
	}
}

func run() error {
	key := []byte(os.Getenv("SQS_SIGNING_KEY"))
	if len(key) < sqsauth.MinKeyLength {
		return fmt.Errorf("SQS_SIGNING_KEY must have at least %d bytes", sqsauth.MinKeyLength)
	}
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	env, err := sqsconsumer.Decode(string(body))
	if err != nil {
		return err
	}
	if err := sqsconsumer.Sign(&env, key); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(env)
}
