
```
sudo apt update

sudo apt upgrade
```

https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html

```
sudo apt install unzip
```

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

setting up a aws profile.. aaws cli, python, node.js....
https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html


```
cd ~

mkdir .aws

touch ~/.aws/credentials

touch ~/.aws/config
```

setup the credentials

```
nano ~/.aws/credentials
```

and paste below

```
[default]
aws_access_key_id=YOURKEY_HERE
aws_secret_access_key=YOUR_SECRET_KEY
```

default region

```
nano ~/.aws/config
```

```
[default]
region=us-east-2
```


```
aws s3 ls
```


Try to setup a package. node.js

```

cd ~

curl -fsSL https://deb.nodesource.com/setup_14.x | sudo -E bash -
sudo apt-get install -y nodejs

sudo npm install http-server -g

mkdir www

touch www/index.html

nano www/index.html

```

paste


```html
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>My EC2</title>
</head>
<body>

    <h2>Welcome to My EC2</h2>
    
</body>
</html>


```

```
http-server -p 8080 -c-1 www
```


open the browser, then try http://<<hostip>>.us-east-2.compute.amazonaws.com:8080/

Install Later

Python/mini-conda
