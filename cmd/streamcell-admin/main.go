// Command streamcell-admin performs provider administration against the database:
//
//	streamcell-admin register-cell cell.json
//	streamcell-admin register-tenant <id> <name>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/avilashs/kafka_as_service/internal/platform/store"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: streamcell-admin register-cell <file.json> | register-tenant <id> <name>")
		os.Exit(2)
	}
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost/streamcell"
	}
	st, err := store.Open(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer st.Close()
	switch os.Args[1] {
	case "register-cell":
		b, err := os.ReadFile(os.Args[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var c store.CellSpec
		if err := json.Unmarshal(b, &c); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if tok := os.Getenv("CELL_TOKEN"); tok != "" {
			c.AgentToken = tok
		}
		err = st.UpsertCell(ctx, c)
		if err == nil {
			fmt.Println("registered cell", c.ID)
		}
		exit(err)
	case "register-tenant":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "register-tenant <id> <name>")
			os.Exit(2)
		}
		err = st.UpsertTenant(ctx, os.Args[2], os.Args[3], nil)
		if err == nil {
			fmt.Println("registered tenant", os.Args[2])
		}
		exit(err)
	default:
		fmt.Fprintln(os.Stderr, "unknown command", os.Args[1])
		os.Exit(2)
	}
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
