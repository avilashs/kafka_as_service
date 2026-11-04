// Command streamcell-loadtest produces acknowledged load against a cluster and verifies that nothing
// acknowledged was lost (design §16, milestone M4).
//
//	streamcell-loadtest produce -bootstrap H:443 -user U -password P -ca ca.pem -topic T -ledger L [-rate 200] [-stop-file F]
//	streamcell-loadtest verify  -bootstrap H:443 -user U -password P -ca ca.pem -topic T -ledger L
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avilashs/kafka_as_service/internal/loadgen"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: streamcell-loadtest produce|verify [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	c := loadgen.Conn{}
	var topic, ledger, stopFile string
	var rate, producers, size int
	var duration time.Duration
	fs.StringVar(&c.Bootstrap, "bootstrap", "", "host:port")
	fs.StringVar(&c.User, "user", "", "SCRAM user")
	fs.StringVar(&c.Password, "password", os.Getenv("STREAMCELL_LOADTEST_PASSWORD"), "SCRAM password (or $STREAMCELL_LOADTEST_PASSWORD)")
	fs.StringVar(&c.CAFile, "ca", "", "PEM CA file")
	fs.StringVar(&c.DialAddr, "dial", "", "connect every broker address here (for hosts that resolve to a local relay)")
	fs.StringVar(&topic, "topic", "", "topic")
	fs.StringVar(&ledger, "ledger", "", "ledger file")
	fs.IntVar(&rate, "rate", 200, "records per second")
	fs.IntVar(&producers, "producers", 2, "producer clients")
	fs.IntVar(&size, "size", 128, "payload bytes")
	fs.DurationVar(&duration, "duration", 0, "stop after (0: until -stop-file or SIGTERM)")
	fs.StringVar(&stopFile, "stop-file", "", "stop when this file exists")
	_ = fs.Parse(os.Args[2:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "produce":
		if stopFile != "" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			defer cancel()
			go func() {
				for ctx.Err() == nil {
					if _, err := os.Stat(stopFile); err == nil {
						cancel()
						return
					}
					time.Sleep(200 * time.Millisecond)
				}
			}()
		}
		res, err := loadgen.Produce(ctx, c, loadgen.ProduceOptions{Topic: topic, Producers: producers, Rate: rate, Size: size, Ledger: ledger, Duration: duration})
		fmt.Printf("produced: sent=%d acked=%d failed=%d", res.Sent, res.Acked, res.Failed)
		if res.FirstError != "" {
			fmt.Printf(" (first error: %s)", res.FirstError)
		}
		fmt.Println()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "verify":
		rep, err := loadgen.Verify(ctx, c, topic, ledger)
		fmt.Println(rep.String())
		for _, l := range [][]string{rep.Lost, rep.Duplicated, rep.Corrupt, rep.Reordered} {
			for i, s := range l {
				if i >= 10 {
					fmt.Println("  …")
					break
				}
				fmt.Println("  ", s)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if !rep.OK() {
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: streamcell-loadtest produce|verify [flags]")
		os.Exit(2)
	}
}
