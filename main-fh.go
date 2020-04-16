package main

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"sync"
	//"io"
	//"strings"
	//"bytes"
	//"encoding/json"
	"time"
	//"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/firehose"
)

type Data struct {
	Text string	`json: "text"`
	Content_id int	`json: "content_id"`
	Client_id int	`json: "client_id"`
	Timestamp time.Time	`json: "timestamp"`
}

func main() {
	//creating a session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-2"),
		//Credentials: credentials.NewSharedCredentials("", "gotest"),
	})
	if err != nil {
		panic(err)
	}
	//check if aws credentials are found
	{
		_, err := sess.Config.Credentials.Get()
		if err != nil {
			panic(err)
		}
	}
	//create s3 uploader
	/*uploader := s3manager.NewUploader(sess)
	if uploader == nil {
		panic("uploader not created")
	}*/

	//Creating service s3
	//svc := s3.New(sess)

	//Creating firehose client
	svcfh := firehose.New(sess)

	//reader, writer := io.Pipe()
	//Iniate multipart upload
	/*inputInit := &s3.CreateMultipartUploadInput{
		Bucket:	aws.String("chatdata-h"),
		Key:	aws.String("/chat/2020-04-14/content_logs_2020-04-14_1"),
		//ContentType:	aws.String("application/octet-stream"),
	}
	result, err := svc.CreateMultipartUpload(inputInit)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("Multipart upload initiated: ",result)

	var completedParts []*s3.CompletedPart
	fmt.Println("Upload ID: ",*result.UploadId)*/
	//fmt.Println("Key      : %s",result.Key)
	
	var wg sync.WaitGroup

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		//reader, writer := io.Pipe()
		if string(ctx.Path()) == "/" {
			fmt.Fprintf(ctx, "Hello, world! Requested path is %q\n", ctx.Path())
		}
		if string(ctx.Path()) == "/json" {
			//fmt.Fprintf(ctx, "You have hit the json endpoint\n")
			fmt.Println(string(ctx.PostBody()))


			/*go func() {
				fmt.Fprint(writer,string(ctx.PostBody()))
				writer.Close()
			}()*/
			
			/*var data Data
			err := json.Unmarshal(ctx.PostBody(),&data)
			if err != nil {
				fmt.Println("error:", err)
			}
			fmt.Println(data.Text)
			date := fmt.Sprintf("%d-%02d-%02d",data.Timestamp.Year(), data.Timestamp.Month(), data.Timestamp.Day())
			key := fmt.Sprintf("/chat/%s/content_logs_%s_%d",date,date,data.Client_id)
			fmt.Println(key)*/
			wg.Add(1)
			go func(){
			defer wg.Done()
			record := &firehose.Record{Data: ctx.PostBody()}
			recInput := firehose.PutRecordInput{
				DeliveryStreamName: aws.String("simpleChatStream"),
				Record: record,
			}

			recResult, recErr := svcfh.PutRecord(&recInput)
			if recErr != nil {
				fmt.Println("failed to put record: ",recErr)
			}
			fmt.Println("record added: ", recResult)
			}()
			wg.Wait()
			//buf := new(bytes.Buffer)
			//buf.ReadFrom(reader)
			//fmt.Println(buf.String())
			/*go func() {
			result, err := uploader.Upload(&s3manager.UploadInput{
				Body: reader,
				Bucket: aws.String("chatdata-h"),
				Key: aws.String(key),
			})
			
			
			if err != nil {
				log.Fatalln("Failed to upload", err)
			}
			log.Println("Successfully uploaded to", result.Location)
			}()*/

			//PUT OBJECT EXAMPLE
			/*input := &s3.PutObjectInput{
				Body:	aws.ReadSeekCloser(strings.NewReader(string(ctx.PostBody()))),
				Bucket: aws.String("chatdata-h"),
				Key:	aws.String(key),
				StorageClass:	aws.String("STANDARD_IA"),
			}
			result, err := svc.PutObject(input)
			if err != nil {
				fmt.Println(err.Error())
			}
			log.Println("Successfully uploaded to", result)*/
			//MULTIPART UPLOAD
			/*input := &s3.UploadPartInput{
				Body:	aws.ReadSeekCloser(reader),
				Bucket:	aws.String("chatdata-h"),
				Key:	result.Key,
				PartNumber:	aws.Int64(int64(data.Content_id)),
				UploadId:	aws.String(*result.UploadId),
			}
			upResult, upErr := svc.UploadPart(input)
			if upErr != nil {
				fmt.Println(upErr.Error())
			}
			fmt.Println("part uploaded: ",upResult)

			completedParts = append(completedParts,
						&s3.CompletedPart{
							ETag:	upResult.ETag,
							PartNumber: aws.Int64(int64(data.Content_id)),
						},)
			//check if end of content is reached
			if data.Content_id == 20 {
				inputComplete := &s3.CompleteMultipartUploadInput{
					Bucket:	aws.String("chatdata-h"),
					Key:	aws.String(key),
					MultipartUpload:	&s3.CompletedMultipartUpload{
						Parts: completedParts,
					},
					UploadId:		aws.String(*result.UploadId),
				}
				
				completeResults, completeErr := svc.CompleteMultipartUpload(inputComplete)
				if completeErr != nil {
					fmt.Println(completeErr.Error())
				}
				fmt.Println("Upload completed successfully: ",completeResults)
			}*/
		}
		//buf := new(bytes.Buffer)
		//buf.ReadFrom(reader)
		//fmt.Println(buf.String())
	}

	s:= &fasthttp.Server{
		Handler: requestHandler,

		Name: "My seper server",

		Concurrency: 1024 * 1024,
	}

	if err := s.ListenAndServe("127.0.0.1:8000"); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}
