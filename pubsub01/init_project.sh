DEVSHELL_PROJECT_ID=mllkthdbt
TOPICID=sandiego


echo $DEVSHELL_PROJECT_ID
# create if not exits
# gcloud pubsub topics create $TOPICID --project=$DEVSHELL_PROJECT_ID

# create subsription
gcloud pubsub subscriptions create --project=$DEVSHELL_PROJECT_ID --topic $TOPICID mySub1
