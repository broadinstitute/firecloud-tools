#!/usr/bin/env bash
if [[ $1 =~ \.json$ ]]; then
    echo "Authorizing service account using json file"
    gcloud auth activate-service-account --key-file=$1
    export GOOGLE_APPLICATION_CREDENTIALS=$1
elif [[ $1 =~ "@" ]]; then
    echo "Authorizing using email address"
    gcloud auth login $1
    export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/legacy_credentials/$1/adc.json
else
    echo "Run this script in the following manner:"
    echo "   To auth as a regular user:    '. auth.sh <your email>'"
    echo "   To auth as a service account: '. auth.sh <path to service account json>'"
    echo "NOTE:  The dot before auth.sh important as this sets an environment variable"
fi