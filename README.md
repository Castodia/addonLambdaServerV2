## to save layers

- run `bash ./create_layer.sh` in layers/castodia
- update layer version in update_functions.sh (layer:castodia:37 to layer:castodia:38, for example)
- run `bash ./update_layers.sh` to update all lambdas

## to upload layer

- run bash ./pushblish.sh _lambdaName_ (example: `./pushblish.sh manage` to upload manageController)
