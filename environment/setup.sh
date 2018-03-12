set -e
gcloud config set component_manager/disable_update_check true
(cd "$(dirname "$0")"
# only do the setup if it has not already been done
if [ ! -d ~/.firecloud-env.config ]; then
    #TODO: if they have multiple identities ask if they are using the right one
    #Check if gcloud is installed 
    if gcloud version | grep -q "gcloud: command not found"; then
        echo
        echo "You do not have Google Cloud SDK installed, which you need to run this script."
        while read -p "Do you want to install gcloud SDK? (yes or no) " yn; do
            #install gcloud, or not
            case $yn in
                [Yy]* ) 
                    curl https://sdk.cloud.google.com | bash
                    exec -l $SHELL
                    gcloud init
                    break;;

                [Nn]* )
                    echo "Exiting."
                    exit 1 
                    break;;

                * ) echo "Please answer yes or no.";;
            #install gcloud, or not
            esac 
        done                                  
    fi
    echo
    echo
    while read -p "Do you have an existing Google project where you want to run Cromwell workflows? (yes or no) " yn; do
        #Which Google project to use (new or existing) 
        case $yn in
            [Yy]* ) read -p "Enter your project name: " project; break;;
            [Nn]* ) 
                echo
                echo "If you do not have a project you want to use, a new one will be generated for you."
                while read -p "Would you like to continue? (yes or no) " yn; do
                    #Continue and create new project, or not
                    case $yn in
                       [Yy]* )
                            
                            echo
                            echo
                            echo "You have access to the following billing accounts."
                            echo "--------------------------------------------------------------------------------"
                            gcloud alpha billing accounts list
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
                            echo
                            echo "Enter the billing account ID to use for this project" 
                            read -p "    (IDs will look similar to this: 002481-B7351F-CD111E):" account
                            #gcloud projects create
                            project="fc-env-$(date +%H-%M-%S)-$(gcloud config get-value account | sed 's/@.*//')"
                            echo
                            gcloud projects create $project
                            echo "Linking project to your billing account..."
                            gcloud alpha billing accounts projects link $project --billing-account=$account
                            sleep 10s
                            echo
                            echo "Project created and can be viewed at: https://console.cloud.google.com/home/dashboard?project=$project"

                            break;;                        
                            
                       [Nn]* ) 
                            echo "Exiting."
                            exit 1 
                            break;;

                        * ) echo "Please answer yes or no.";;

                    #Continue and create new project, or not
                    esac 

                done

                break;;
            
            * ) echo "Please answer yes or no.";;
    
        #Which Google project to use (new or existing)
        esac 
    done 
    
    #create bucket
    bucket=$project-executions
    echo "gs://$bucket"
    gsutil mb -p $project gs://$bucket
    echo "Bucket created for Cromwell execution outputs can be viewed at: https://console.cloud.google.com/storage/browser/$bucket"
    #TODO: ask for dockerhub credentials if they are going to use private dockers

    #TODO: create config with: execution bucket, project, optional Dockerhub credentials

    echo $project  
    #read varname
else
    echo "Setup has already been done.  If you would like to clear this setup and create"
    echo "a new one, you can remove the file ~/.firecloud-env.config"
fi) && gcloud config set component_manager/disable_update_check false

