CA_PASSWORD?=strong_password
PASSWORD?=strong_password
OUTPUT_FOLDER=.

ssl_keys: clear_keys
	@./create_keys.sh

clear_keys:
	@(cd $(OUTPUT_FOLDER) && rm -f *.key *.crt *.jks \
	*.csr *.pem *.p12 *.srl extfile)

.PHONY: ssl_keys