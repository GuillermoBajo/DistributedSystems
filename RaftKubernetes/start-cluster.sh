# echo -e "Deleting cluster"
# ./delete-cluster.sh

# echo -e "\Creating cluster"
# ./kind-with-registry.sh

echo -e "\nCompiling clinet"
rm  ./cliente/cltraft >/dev/null 2>&1
cd ./raft
CGO_ENABLED=0 go build -o ./../cliente/cltraft ./pkg/cltraft/cltraft.go
docker build ./../cliente -t localhost:5001/cliente:latest
docker push localhost:5001/cliente:latest
cd ..

echo -e "\nCompilando servidor"
rm  ./servidor/srvraft >/dev/null 2>&1
cd ./raft
CGO_ENABLED=0 go build -o ./../servidor/srvraft ./cmd/srvraft/main.go
docker build ./../servidor -t localhost:5001/servidor:latest
docker push localhost:5001/servidor:latest
cd ..

echo -e "\nLanzando Kubernetes"
kubectl delete statefulset raft
kubectl delete pod client
kubectl delete service service-raft
kubectl create -f raft_go.yml
