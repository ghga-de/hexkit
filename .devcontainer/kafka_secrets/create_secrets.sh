#!/bin/bash

# Create Kafka secrets used in integration tests

set -e
set -u

cd $(dirname "${BASH_SOURCE[0]}")
[ -f create_secrets.sh ] || exit 1
if [ -f ca.crt -o -f client.crt ]
then
  echo "Secrets already exist. You can remove them to create new ones."
  exit 0
fi

# install openssl and keytool
if ! command -v openssl &> /dev/null || ! command -v keytool &> /dev/null
then
  if ! apt install -y openssl openjdk-11-jre-headless
  then
    echo Please install openssl and keytool.
    exit 1
  fi
fi

# remove old secrets
rm -f *.crt *.csr *.key *.srl *.jks *.p12

pw=testpw
echo $pw > pwd.txt

echo "Generate a CA key..."
openssl req -new -x509 -keyout ca.key -out ca.crt -days 9999 \
  -subj '/CN=ca.test.ghga.dev/OU=TEST/O=GHGA' \
  -passin pass:$pw -passout pass:$pw

for component in broker client
do
  echo "Create keystore for Kafka $component..."
  keytool -genkey -noprompt -alias $component \
    -dname "CN=localhost, OU=TEST, O=GHGA" \
    -keystore $component.keystore.jks \
    -keyalg RSA -storepass $pw -keypass $pw

  # create CSR, sign the key and import back into keystore
  keytool -keystore $component.keystore.jks -alias $component -certreq \
    -file $component.csr -storepass $pw -keypass $pw

  openssl x509 -req -CA ca.crt -CAkey ca.key \
      -in $component.csr -out $component.crt \
      -days 9999 -CAcreateserial -passin pass:$pw

  keytool -keystore $component.keystore.jks -alias CARoot \
    -import -file ca.crt -storepass $pw -keypass $pw -noprompt

  keytool -keystore $component.keystore.jks -alias $component \
    -import -file $component.crt -storepass $pw -keypass $pw -noprompt

  # create truststore and import the CA cert
  keytool -keystore $component.truststore.jks -alias CARoot \
    -import -file ca.crt -storepass $pw -keypass $pw -noprompt
done

# Create certfile and encrypted keyfile for the client
keytool -importkeystore -srckeystore client.keystore.jks -srcalias client \
   -destkeystore client.keystore.p12 -deststoretype PKCS12 \
   -srcstorepass $pw -deststorepass $pw -noprompt
openssl pkcs12 -in client.keystore.p12 -nocerts -out client.key \
  -passin pass:$pw -passout pass:$pw

rm -f broker.crt broker.key ca.key *.csr *.p12 *.srl
