####端口
占用端口5233

####依赖
go mod不需要显式依赖GOPATH环境 \
go mod download 下载依赖 \
or \
go mod tidy 移除未用的模块，以及添加缺失的模块


####打包&build
推荐使用脚本
sh builder.sh

or 

linux平台打包程二进制执行文件\
CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o rabbitmq_test .

算法编译\
cd ${project path}/consumer/cpp
sh build.sh

docker打包镜像\
docker build -t rabbitmq_test .

