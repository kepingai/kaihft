# Welcome to Keping AI High Frequency Trading 
This is the source code for KepingAI High Frequency Trading signal product. This is the implementation of KepingAI Signal Engine (Layer 1) that is responsible for generating actionable trading signal via technical analysis thresholds. This layer should be communiating to our Intelligence Layer (Layer 2) to make specific forecasting of the current trading strategy.

# Initializing Git, Screen & Pip3
```
$ sudo apt install git-all
$ sudo apt-get install screen
$ sudo apt-get install python3-pip
$ pip3 install --upgrade pip
```
After successfuly install git and pip please reset the instance. Note, you don't have to do this if you are not initializing a new google vm instance. If so please skip to the next step.

# Setup environment
```
# clone or git pull from origin
$ git clone https://github.com/kepingai/kaihft.git
$ cd kaihft
# create a tmp file to write/delete temporary files
$ mkdir tmp
$ pip3 install -r requirements.txt
```

# Run Locally
Make sure that you are setting the `GOOGLE_APPLICATION_CREDENTIALS` in your environment to the specific credential json your supervisor give you. This will grant the application the overall accesibility of KepingAI's google services.
```
$ GOOGLE_APPLICATION_CREDENTIALS='credentials.json' python3 runner.py ticker-binance-spot
```