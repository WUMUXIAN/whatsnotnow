package main

import (
	"context"
	"fmt"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	conn, err := kafka.DialContext(context.Background(), "tcp", "localhost:29092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Create two test topics, one with 1 partition, 1 with 50 partitions
	err = conn.CreateTopics(
		kafka.TopicConfig{
			Topic:             "test_topic_1",
			NumPartitions:     50,
			ReplicationFactor: 1,
		}, kafka.TopicConfig{
			Topic:             "test_topic_2",
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	if err != nil {
		panic(err)
	}

	// Let's now try out some functions of the conn.
	fmt.Println("conn.LocalAddr()->", conn.LocalAddr())
	fmt.Println("conn.RemoteAddr()->", conn.RemoteAddr())

	// Write 10,000 messages to the broker.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   "test_topic_2",
	})
	msgs := []kafka.Message{}
	for i := 0; i < 10000; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		})
	}
	writer.WriteMessages(context.Background(), msgs...)

	fmt.Print("writer.Stats()-> ")
	fmt.Println(writer.Stats())
	defer writer.Close()

	// Read messages with a consume group.
	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:29092"},
		GroupID:  "consumer_group_1",
		Topic:    "test_topic_2",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()

}

//
// func main() {
// 	flags := flag.NewFlagSet("user-auth", flag.ExitOnError)
// 	consumerKey := flags.String("consumer-key", "", "Twitter Consumer Key")
// 	consumerSecret := flags.String("consumer-secret", "", "Twitter Consumer Secret")
// 	accessToken := flags.String("access-token", "", "Twitter Access Token")
// 	accessSecret := flags.String("access-secret", "", "Twitter Access Secret")
// 	flags.Parse(os.Args[1:])
// 	flagutil.SetFlagsFromEnv(flags, "TWITTER")
//
// 	if *consumerKey == "" || *consumerSecret == "" || *accessToken == "" || *accessSecret == "" {
// 		log.Fatal("Consumer key/secret and Access token/secret required")
// 	}
//
// 	config := oauth1.NewConfig(*consumerKey, *consumerSecret)
// 	token := oauth1.NewToken(*accessToken, *accessSecret)
// 	// OAuth1 http.Client will automatically authorize Requests
// 	httpClient := config.Client(oauth1.NoContext, token)
//
// 	// Twitter Client
// 	client := twitter.NewClient(httpClient)
//
// 	fmt.Println("Starting Stream...")
//
// 	// We use the sample API to get random tweet feeds from Twitter.
// 	sampleParams := &twitter.StreamSampleParams{
// 		StallWarnings: twitter.Bool(true),
// 	}
// 	stream, err := client.Streams.Sample(sampleParams)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
//
// 	// make a writer that produces to topic-A, using the least-bytes distribution
// 	w := kafka.NewWriter(kafka.WriterConfig{
// 		Brokers:  []string{"localhost:9092"},
// 		Topic:    "twitter",
// 		Balancer: &kafka.LeastBytes{},
// 		Async:    true,
// 	})
//
// 	totalCount := 0
//
// 	// demultiplexed messages, we are only interested in Tweets
// 	demux := twitter.NewSwitchDemux()
// 	demux.Tweet = func(tweet *twitter.Tweet) {
// 		fmt.Println("Tweet Text:", tweet.Text)
// 		totalCount += 1
// 		w.WriteMessages(context.Background(),
// 			kafka.Message{
// 				Value: []byte(tweet.Text),
// 			},
// 		)
// 	}
//
// 	startTime := time.Now()
// 	// Receive messages until stopped or stream quits
// 	go demux.HandleChan(stream.Messages)
//
// 	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
// 	ch := make(chan os.Signal)
// 	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
// 	log.Println(<-ch)
//
// 	fmt.Println("Stopping Stream...")
// 	stream.Stop()
//
// 	endTime := time.Now()
//
// 	fmt.Println("Tweet Per Second Is:", float64(totalCount)/float64(endTime.Unix()-startTime.Unix()), "per second")
//
// 	w.Close()
// }
