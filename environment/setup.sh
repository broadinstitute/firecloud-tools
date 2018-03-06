set -e
cd "$(dirname "$0")"
# only do the setup if it has not already been done
if [ ! -d ~/.firecloud-env.config ]; then
    #TODO: handle if gcloud is not installed, if they have multiple identities ask if they are using the right one
    read -p "Do you have an existing Google project where you want to run Cromwell workflows? (yes or no) " yn
    case $yn in
        [Yy]* ) read -p "Enter your project name: " project;;
        [Nn]* ) 
            echo
            echo "If you do not have a project you want to use, a new one will be generated for you."
            read -p "Would you like to continue? (yes or no) " yn
            case $yn in
               [Yy]* ) 
                    accounts=$(gcloud alpha billing accounts list 2>&1)
                    
                    if echo $accounts | grep -q "Listed 0 items"; then 
                        echo
                        echo "You do not have a Google billing account setup.  In order to run "
                        echo "WDLs in the Google cloud you need an account to bill to.  See the README "
                        echo "for more details."
                        echo "To learn about creating a billing account, see here: "
                        echo "https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account"
                        exit 1
                    fi
                    
                    echo 
                    echo "Please choose a billing account to bill this new project to from the list below "
                    echo "by providing it's ID.  IDs will look similar to this: 002481-B7351F-CD111E"
                    echo "--------------------------------------------------------------------------------"
                    gcloud alpha billing accounts list
                    echo
                    read -p "Enter the billing account ID to use for this project: " account
                    #gcloud projects create 
                    project="fc-env-$(gcloud config get-value account | sed 's/@.*//')"
                    echo
                    gcloud projects create $project
                    gcloud alpha billing accounts projects link $project --billing-account=$account
                    echo
                    echo "Project created and can be viewed at: https://console.cloud.google.com/home/dashboard?project=$project"
                    ;;
                    
                    gcloud config set project $project
                    gsutil 
                    
                    #TODO: create an execution bucket, 
                    #TODO: ask for dockerhub credentials if they are going to use private dockers
                    #TODO: create config with: execution bucket, project, 
               [Nn]* ) 
                    echo "Exiting."
                    exit 1 
                    ;;
            esac 
            ;;
            
        * ) echo "Please answer yes or no.";;
    esac  
    
    echo $project  
    #read varname
else
    echo "Setup has already been done.  If you would like to clear this setup and create"
    echo "a new one, you can remove the file ~/.firecloud-env.config"
fi
