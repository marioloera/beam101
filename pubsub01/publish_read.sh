DEVSHELL_PROJECT_ID=mllkthdbt
TOPICID=sandiego

# publish
# gcloud pubsub topics publish --project=$DEVSHELL_PROJECT_ID $TOPICID --message "hello"


for i in {1..3}
do
   # echo "Welcome $i times"
   gcloud pubsub topics publish --project=$DEVSHELL_PROJECT_ID $TOPICID --message "hello$i "

done

# read
for i in {1..3}
do
    gcloud pubsub subscriptions pull --auto-ack --project=$DEVSHELL_PROJECT_ID mySub1
done

