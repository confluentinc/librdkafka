# Only for logging purposes
node -v
uname -m

installDir=$(mktemp -d)
cd $installDir
npm init -y

# uncomment for one final run.
library_version="$2"

if [ -z "${library_version}" ]; then
    library_version="latest"
fi

npm install @confluentinc/kafka-javascript@${library_version} --save
node -e 'console.log(require("@confluentinc/kafka-javascript").librdkafkaVersion);'
