docker build --tag func-mcr-linux:v1.0.0 .

docker login crfdadecoderdev001.azurecr.io --username crfdadecoderdev001 --password 5guBqt/ixtr10RdA+ZzhDoFLEt0LXCEyexcw4Qoa2A+ACRBAl8+C

docker tag func-mcr-linux:v1.0.0 crfdadecoderdev001.azurecr.io/func-mcr-linux:v1.0.0

docker push  crfdadecoderdev001.azurecr.io/func-mcr-linux:v1.0.0 