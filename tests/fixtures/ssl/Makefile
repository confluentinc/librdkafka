ssl_keys: clear_keys
	@CNS="client client2" ./create_keys.sh

clear_keys:
	@rm -f *.key *.crt *.jks \
	*.csr *.pem *.p12 *.srl extfile

.PHONY: ssl_keys
