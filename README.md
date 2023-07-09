# gosl
<hr style="border:1px solid #444; margin-top: -0.5em;">  

`gosl` is a library that provides common golang's sql builder.  
This README is still not fully completed yet, will be updated shortly.
The codes were created by [@johnjerrico](https://github.com/johnjerrico), published here so it can be used publicly.

### Installation
<hr style="border:1px solid #444; margin-top: -0.5em;">  

Get the code with:
```
$ go get github.com/louvri/gosl
```
### Usage
<hr style="border:1px solid #444; margin-top: -0.5em;">  

On your request model add the transformer if you want:
```
type Request struct {
	...
	Transformer *transformer.Transformer
}
```
Then put it at your request object creation/modification:
```
...
    Object: search.Request{
        ...,
        Transformer: &transformer.Transformer{
            Store: func(data interface{}) error {
                return request.Transformer.Store(data)
            },
            Transform: func(data interface{}) (interface{}, error) {
                tmp := data.(dbModel.Model)
                tmp.Status = constant.StatusMap[tmp.Status]
                transformedSales, _ := request.Transformer.Transform(tmp)
                return transformedSales, nil
            },
        },
    }
...
```
And call it within the sql retrieval codes:
```
    ...
    for rows.Next() {
        ...
        _, err = obj.Transform(result)
        if err != nil {
            return err
        }
        ...
    }
    ...
```
