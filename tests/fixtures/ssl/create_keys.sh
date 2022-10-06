#!/bin/sh
set -e
CA_PASSWORD="${CA_PASSWORD:-strong_password}"
PASSWORD="${PASSWORD:-$CA_PASSWORD}"
OUTPUT_FOLDER=${OUTPUT_FOLDER:-.}
CNS=${CNS:-client}

cd ${OUTPUT_FOLDER}
CA_ROOT_KEY=caroot.key
CA_ROOT_CRT=caroot.crt

echo "# Generate CA"
openssl req -new -x509 -keyout $CA_ROOT_KEY \
-out $CA_ROOT_CRT -days 365 -subj \
'/CN=caroot/OU=/O=/L=/ST=/C=' -passin "pass:${CA_PASSWORD}" \
-passout "pass:${CA_PASSWORD}"

for CN in $CNS; do
    KEYSTORE=kafka.$CN.keystore.jks
    TRUSTSTORE=kafka.$CN.truststore.jks
    KEYSTORE_P12=$CN.keystore.p12
    SIGNED_CRT=$CN-ca-signed.crt
    CERTIFICATE=$CN.certificate.pem
    KEY=$CN.key

    echo "# $CN: Generate JKS Keystore"
    keytool -genkey -noprompt \
        -alias $CN \
        -dname "CN=$CN,OU=,O=,L=,S=,C=" \
                        -ext "SAN=dns:$CN,dns:localhost" \
        -keystore $KEYSTORE \
        -keyalg RSA \
        -storepass "${PASSWORD}" \
        -keypass "${PASSWORD}" \
        -storetype pkcs12

    echo "# $CN: Generate JKS Truststore"
    keytool -noprompt -keystore \
        $TRUSTSTORE -alias caroot -import \
        -file $CA_ROOT_CRT -storepass "${CA_PASSWORD}"\
        -keypass "${PASSWORD}"

    echo "# $CN: Generate CSR"
    keytool -keystore  $KEYSTORE -alias $CN \
        -certreq -file  $CN.csr -storepass "${PASSWORD}" \
        -keypass "${PASSWORD}" -ext "SAN=dns:$CN,dns:localhost"

    echo "# $CN: Generate extfile"
    cat << EOF > extfile
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = $CN
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = $CN
DNS.2 = localhost
EOF

    echo "# $CN: Sign the certificate with the CA"
    openssl x509 -req -CA $CA_ROOT_CRT -CAkey $CA_ROOT_KEY \
        -in $CN.csr \
        -out $CN-ca-signed.crt -days 9999 \
        -CAcreateserial -passin "pass:${PASSWORD}" \
        -extensions v3_req -extfile extfile

    echo "# $CN: Import root certificate into JKS"
    keytool -noprompt -keystore $KEYSTORE \
        -alias caroot -import -file $CA_ROOT_CRT -storepass "${PASSWORD}"\
        -keypass "${PASSWORD}"

    echo "# $CN: Import signed certificate into JKS"
    keytool -noprompt -keystore $KEYSTORE -alias $CN \
        -import -file $SIGNED_CRT -storepass "${PASSWORD}" \
        -keypass "${PASSWORD}" \
        -ext "SAN=dns:$CN,dns:localhost"

    echo "# $CN: Convert JKS to PKCS#12"
    keytool -importkeystore -srckeystore "$KEYSTORE" -destkeystore \
        "$KEYSTORE_P12" -deststoretype PKCS12 -deststorepass "${PASSWORD}" \
        -srcstorepass "${PASSWORD}" -noprompt

    echo "# $CN: Export PEM certificate"
    openssl pkcs12 -in "$KEYSTORE" -out "$CERTIFICATE" \
        -nodes -passin "pass:${PASSWORD}"

    echo "# $CN: Export PEM key"
    openssl pkcs12 -in "$KEYSTORE" -out "$KEY" \
        -nocerts -passin "pass:${PASSWORD}" \
        -passout "pass:${PASSWORD}"
done
