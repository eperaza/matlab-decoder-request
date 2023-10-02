dev

docker build --tag func-mcr-linux:v1.0.0 .

docker login crfdadecoderdev001.azurecr.io --username crfdadecoderdev001 --password 5guBqt/ixtr10RdA+ZzhDoFLEt0LXCEyexcw4Qoa2A+ACRBAl8+C

docker tag func-mcr-linux:v1.0.0 crfdadecoderdev001.azurecr.io/func-mcr-linux:v1.0.0

docker push  crfdadecoderdev001.azurecr.io/func-mcr-linux:v1.0.0 

test

docker build --tag matlab-windows2019:v1.0 .

docker login acrtspservicestest.azurecr.io --username acrtspservicestest --password dJfL8YFsQ0TMiaFVN5jHBa0aJdYi2iFW+VHt51DkZz+ACRCFSFcL

docker tag matlab-windows2019:v1.0 acrtspservicestest.azurecr.io/matlab-windows2019:v1.0

docker push acrtspservicestest.azurecr.io/matlab-windows2019:v1.0
