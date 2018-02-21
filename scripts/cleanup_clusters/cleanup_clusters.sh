#!/usr/bin/env bash

ACTION=$1

if [[ "$ACTION" != "list" && "$ACTION" != "delete" ]]
then
    echo "***ERROR***"
    echo "Please choose 'list' or 'delete' actions and try again."
    echo "***ERROR***"

    exit 1
fi

TEST_ADMIN_SUFFIX='@test.firecloud.org'
QUALITY_ADMIN_SUFFIX='@quality.firecloud.org'

USER=$(gcloud auth list | grep '*' | tr -s ' ' | cut -f2 -d' ')
echo "You are currently logged in as $USER"

if [[ "$USER" != *"$TEST_ADMIN_SUFFIX" && "$USER" != *"$QUALITY_ADMIN_SUFFIX" ]]
then
    echo "***ERROR***"
    echo "This script is intended for running only by test-domain and quality-domain admins."
    echo "Please run 'gcloud auth login' with an email ending in $TEST_ADMIN_SUFFIX or $QUALITY_ADMIN_SUFFIX and try again."
    echo "***ERROR***"

    exit 2
fi

CLUSTER_REGION='us-central1'

for proj in `gcloud projects list --format='table(NAME)[no-heading]'`
    do
        # does this project have dataproc enabled? calling 'gcloud dataproc' will error if not
        DATAPROC=$(gcloud services list --enabled --format='table(NAME)[no-heading]' --project $proj | grep dataproc.googleapis.com)
        ERR=$?
        if [ $ERR -eq 0 ]
        then
            for cluster in `gcloud dataproc clusters list --region $CLUSTER_REGION --format='table(NAME)[no-heading]' --project $proj`
            do
                # get the UTC time of the last status change for this cluster
                LAST_STATUS=$(gcloud dataproc clusters describe --format='table(STATUS.stateStartTime)[no-heading]' --project $proj --region us-central1 $cluster | cut -f1 -d'.')

                # NOTE! this assumes the BSD date command on OS X.  Will likely need tweaks for GNU date on Linux.
                SECONDS_AGO=$(( `date +%s` - `date -j -u -f '%Y-%m-%dT%H:%M:%S' $LAST_STATUS +%s` ))
                DAYS_AGO=$(( $SECONDS_AGO / (60 * 60 * 24) ))

                # $((evaluation)) truncates integers, so this means "within the last day"
                if [ $DAYS_AGO -eq 0 ]
                then
                    echo "Not deleting cluster $cluster in $proj ... last updated less than one full day ago"
                else
                    if [[ "$ACTION" == "delete" ]]
                    then
                        echo "Deleting old cluster $cluster in $proj ... last updated $DAYS_AGO days ago"
                        yes | gcloud dataproc clusters delete --async --region $CLUSTER_REGION --project $proj $cluster
                        echo
                    else
                        echo "Would delete old cluster $cluster in $proj ... last updated $DAYS_AGO days ago"
                    fi
                fi
            done
        fi
    done