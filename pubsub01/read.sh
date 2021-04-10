DEVSHELL_PROJECT_ID=mllkthdbt
# gcloud pubsub subscriptions pull --auto-ack --project=$DEVSHELL_PROJECT_ID mySub1

# read
for i in {1..300}
do
    gcloud pubsub subscriptions pull --auto-ack --project=$DEVSHELL_PROJECT_ID mySub1
done

