# opensearch-spring-boot-starter

springboot-starter of opensearch

| name       | Version | 
|:-----------|:-------:| 
| SpringBoot |  3.0.2  |
| OpenSearch |  2.6.0  | 
| opensearch-java |  2.2.0  | 
| JUnit      |  5.9.1  | 

# FAQ
## Question1

If we config https protocol in application.yml
``` 
opensearch:
  schema: https 
``` 

We maybe meet this error message as below:

``` 
    Host name 'xxx' does not match the certificate subject provided by the peer 
``` 

 The reason for this problem is that the `Hostname` returned by the certificate is a domain name, but the ip address is used for authentication.
 
### Solution
Replace `https://[ip address]` with the `Hostname` in the certificate returned by the server.

Then add the mapping of `ip` to `Hostname` in the machine's hosts file. This will ensure that the script will not be interrupted by "`Hostname does not match`" during automatic execution.

## Question2
When we run the unit test code in the test directory, we will encounter the following exceptions

``` 
sun.security.validator.ValidatorException: PKIX path validation failed: java.security.cert.CertPathValidatorException: validity check failed。
``` 
### Solution

Use the Java keytool to create a custom truststore and import the root authority certificate. The keytool does not understand the .pem format, so you’ll have to first convert the root authority certificate to .der format using openssl cryptography library and then add it to the custom truststore using Java keytool. Most Linux distributions already come with openssl installed.

#### Step 1

Convert the root authority certificates from .pem to .der format.

for example, I login the one node of cluster

```
openssl x509 -in /opt/opensearch-2.6.0/config/root-ca.pem -inform pem -out root-ca.der -outform der
``` 

#### Step 2
Create a custom truststore and add the root-ca.der certs.

Adding the root authority certificate to the application truststore tells the application to trust any certificate signed by this root CA.


```
keytool -importcert -file root-ca.der -alias opensearch -keystore wjunshenStore
``` 

Confirm the action was successful by listing certs in truststore. The grep should be able to find opensearch alias if the certs were added successfully.

here, I used my alias `wjunshen` as password
```
keytool -keystore wjunshenStore -storepass wjunshen -list | grep opensearch
``` 

#### Step 3

Set the truststore properties in the Java application code to point to the custom truststore

```
mv wjunshenStore /home/ec2-user/

scp -i "wjunshen.pem" ec2-user@ec2-35-72-115-65.ap-northeast-1.compute.amazonaws.com:/home/ec2-user/wjunshenStore ~/Downloads/keystore //复制目录
``` 
Then change the code in `OpenSearchAutoConfiguration.java`

```
// Point to keystore with appropriate certificates for security.
System.setProperty("javax.net.ssl.trustStore", "/full/path/to/myCustomTrustStore");
System.setProperty("javax.net.ssl.trustStorePassword", "password-for-myCustomTrustStore");
``` 

and I also set these 2 items in `application.yml` file, then the code will be changed as below:

```
 System.setProperty("javax.net.ssl.trustStore", sslConfigProperties.getPath());
 System.setProperty("javax.net.ssl.trustStorePassword", sslConfigProperties.getPassword());
``` 

All of the above is my reference to this blog https://opensearch.org/blog/connecting-java-high-level-rest-client-with-opensearch-over-https/, after testing and verifying according to the scenarios I encountered. I hope it can help readers