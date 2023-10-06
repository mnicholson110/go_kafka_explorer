package main

import (
	"fmt"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/go-chi/chi/v5"
)

func main() {
	r := chi.NewRouter()

	r.Get("/static/*", func(w http.ResponseWriter, a *http.Request) {
		http.ServeFile(w, a, "./static/"+chi.URLParam(a, "*"))
	})

	r.Get("/", func(w http.ResponseWriter, a *http.Request) {
		http.ServeFile(w, a, "./static/index.html")
	})

	r.Post("/post", func(w http.ResponseWriter, a *http.Request) {
		// fmt.Println(a.FormValue("bootstrap"))

		admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
			"bootstrap.servers": a.FormValue("bootstrap"),
			"sasl.username":     a.FormValue("username"),
			"sasl.password":     a.FormValue("password"),
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms":   "PLAIN",
		})
		if err != nil {
			panic(err)
		}
		defer admin.Close()

		md, err := admin.GetMetadata(nil, true, 5000)
		if err != nil {
			panic(err)
		}

		for _, topic := range md.Topics {
			fmt.Println("Topic: ", topic.Topic)
			for _, partition := range topic.Partitions {
				fmt.Println("Parition ID:", partition.ID)
				fmt.Println("Partition Leader: Broker", partition.Leader)
				fmt.Println("Replicas:", partition.Replicas)
				fmt.Println("Isrs:", partition.Isrs)
			}
		}

		schema, err := schemaregistry.NewClient(&schemaregistry.Config{
			SchemaRegistryURL:          a.FormValue("schema"),
			BasicAuthUserInfo:          a.FormValue("schemaUsername") + ":" + a.FormValue("schemaPassword"),
			BasicAuthCredentialsSource: "USER_INFO",
		})
		if err != nil {
			panic(err)
		}

		fmt.Println("Schema Registry: ")
		schemaMeta, err := schema.GetLatestSchemaMetadata("topic_0-value")
		if err != nil {
			panic(err)
		}
		fmt.Println(schemaMeta.SchemaInfo)
	})

	fmt.Println("Server running")

	http.ListenAndServe(":3000", r)
}
