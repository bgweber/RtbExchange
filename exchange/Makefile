
build:
	docker build --tag exchange .

deploy:
	docker tag exchange us.gcr.io/logical-tea-123/exchange
	gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://us.gcr.io
	docker push us.gcr.io/logical-tea-123/exchange
