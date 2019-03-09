# simple-mapreduce

## Install and Setup

Для запуска системы необходим кластер `kubernetes`. Как развернуть кластер локально, можно прочитать на [официальном сайте](https://kubernetes.io/docs/tasks/tools/install-minikube/).

После того, как вы развернули кластер и подключились к нему (для доступа через `kubectl`), вам нужно выполнить 3 команды:

```
kubectl create -f mapreduce-reduce.yaml
kubectl create -f mapreduce-map.yaml
kubectl create -f mapreduce-master.yaml
```

Подождав немного времени, пока система развернется, можно начать эксперементировать:

```
kubectl proxy --port=8080 &
curl http://127.0.0.1:8080/api/v1/namespaces/default/pods/mapreduce-master/proxy/compute\?text\=Hello+world+test+test+test+test+test+test+helo+araara+tttt+dddd+araara+test+hello+hi+ih+ih+ih+hi
```
