package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb"
	"github.com/psanford/sqlite3vfs"
	"github.com/spf13/cobra"
)

var (
	region = "us-east-1"

	verboseOutput bool
)

const (
	fileMetaKey = "file-meta-v1"
	hKey        = "hash_key"
	rKey        = "range_key"
)

var rootCmd = &cobra.Command{
	Use:   "donutdb-cli",
	Short: "DonutDB CLI",
}

func main() {
	if os.Getenv("AWS_DEFAULT_REGION") != "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	rootCmd.AddCommand(lsFilesCommand())
	rootCmd.AddCommand(pullFileCommand())
	rootCmd.AddCommand(pushFileCommand())
	rootCmd.AddCommand(rmFileCommand())
	rootCmd.AddCommand(debugCommand())
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func lsFilesCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "ls <table>",
		Short: "List files in table",
		Run:   lsFilesAction,
	}

	cmd.Flags().BoolVarP(&verboseOutput, "verbose", "v", false, "Show verbose (multi-line) output")

	return &cmd
}

func lsFilesAction(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Fatalf("Usage: ls <dynamodb_table>")
	}

	table := args[0]

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	fileRow, err := dynamoClient.GetItem(&dynamodb.GetItemInput{
		TableName: &table,
		Key: map[string]*dynamodb.AttributeValue{
			hKey: {
				S: aws.String(fileMetaKey),
			},
			rKey: {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		log.Fatalf("GetItem err: %s", err)
	}

	for k, v := range fileRow.Item {
		if k == hKey || k == rKey {
			continue
		}
		fmt.Printf("%s %s\n", k, *v.S)
	}
}

func pullFileCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "pull <table> <filename>",
		Short: "Pull file from dynamodb to local filesystem",
		Run:   pullFileAction,
	}

	return &cmd
}

func pullFileAction(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		log.Fatalf("Usage: ls <dynamodb_table> <file>")
	}

	table := args[0]
	filename := args[1]

	outFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		log.Fatalf("File %s already exists on disk, won't overwrite", filename)
	}
	defer outFile.Close()

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	vfs := donutdb.New(dynamoClient, table)

	file, _, err := vfs.Open(filename, 0)
	if err != nil {
		log.Fatalf("Open file err: %s", err)
	}

	size, err := file.FileSize()
	if err != nil {
		log.Fatalf("Get file size err: %s", err)
	}

	fileReader := io.NewSectionReader(file, 0, size)

	_, err = io.Copy(outFile, fileReader)
	if err != nil {
		outFile.Close()
		os.Remove(filename)
		log.Fatalf("Copy dynamo file to local disk err: %s", err)
	}

	log.Printf("wrote %s\n", filename)
}

func pushFileCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "push <table> <local_file> <remote_file>",
		Short: "Push file from dynamodb to local filesystem",
		Run:   pushFileAction,
	}

	return &cmd
}

func pushFileAction(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		log.Fatalf("Usage: ls <dynamodb_table> <local_file> <remote_file>")
	}

	table := args[0]
	srcFileName := args[1]
	dstFileName := args[2]

	localFile, err := os.Open(srcFileName)
	if err != nil {
		log.Fatalf("Failed to open local file: %s, err: %s", srcFileName, err)
	}
	defer localFile.Close()

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	vfs := donutdb.New(dynamoClient, table)

	file, _, err := vfs.Open(dstFileName, 0)
	if err != nil {
		log.Fatalf("Open file err: %s", err)
	}

	err = file.Truncate(0)
	if err != nil {
		panic(err)
	}

	w := &writerFromWriterAt{
		File: file,
	}

	// use our sector size as our tmp buffer
	buf := make([]byte, 1<<17)

	_, err = io.CopyBuffer(w, localFile, buf)
	if err != nil {
		log.Fatalf("Failed to push file to dynamodb: %s", err)
	}

	log.Printf("pushed %s to %s\n", srcFileName, dstFileName)
}

func rmFileCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "rm <table> <filename>",
		Short: "Remove file from dynamodb table",
		Run:   rmFileAction,
	}

	return &cmd
}

func rmFileAction(cmd *cobra.Command, args []string) {
	if len(args) < 2 {
		log.Fatalf("Usage: ls <dynamodb_table> <file>")
	}

	table := args[0]
	filename := args[1]

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	vfs := donutdb.New(dynamoClient, table)

	err := vfs.Delete(filename, false)
	if err != nil {
		log.Fatalf("Failed to rm file from dynamodb: %s", err)
	}
}

type writerFromWriterAt struct {
	sqlite3vfs.File
	offset int
}

func (w *writerFromWriterAt) Write(p []byte) (int, error) {
	n, err := w.WriteAt(p, int64(w.offset))
	w.offset += n
	fmt.Printf("size: %d\n", w.offset)

	return n, err
}
