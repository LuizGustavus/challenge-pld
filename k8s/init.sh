kind create cluster --config ./manifests/kind-config.yaml

kubectl config view --minify --raw -o json > ./kubeconfig.json

sed -i '/127.0.0.1/c\"server": "https://kind-control-plane:6443",' ./kubeconfig.json

kubectl apply -f ./manifests/pv.yaml
kubectl apply -f ./manifests/pvc.yaml
kubectl apply -f ./manifests/secret.yaml
kubectl apply -f ./manifests/postgres-deployment.yaml
kubectl apply -f ./manifests/postgres-service.yaml

while true; do
    existing_pod=$(kubectl get pods --ignore-not-found -o jsonpath='{.items[0].metadata.name}')

    if [ -n "$existing_pod" ]; then
        break
    else
        echo "Pod still creating"
        sleep 3
    fi
done

POD_NAME=$(kubectl get pods -o jsonpath='{.items[0].metadata.name}')

while true; do
    # Get the current state of the pod
    POD_STATUS=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.phase}')

    # Check if the pod is in the "Running" state
    if [ "$POD_STATUS" == "Running" ]; then
        echo "Pod is now Running."
        break  # Exit the loop when the pod is in the "Running" state
    else
        echo "Pod is not in the Running state. Current state: $POD_STATUS"
    fi

    # Sleep for 3 seconds
    sleep 3
done

# Set the desired service name
SERVICE_NAME="postgres-service"

# Loop until the service exists
while true; do
    service_info=$(kubectl get service "$SERVICE_NAME" --ignore-not-found -o wide)

    if [ -n "$service_info" ]; then
        echo "Service '$SERVICE_NAME' exists:"
        echo "$service_info"
        break
    else
        echo "Service '$SERVICE_NAME' not found. Retrying in 3 seconds..."
        sleep 3
    fi
done
