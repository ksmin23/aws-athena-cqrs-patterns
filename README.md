
# AWS Athena CQRS Pattern
AWS Athena를 활용한 CQRS 패턴으로 구현 예제
- RESTful API로 받은 aws athena query 실행 요청은 받은 후, query 실행 결과를 다운로드 받을 수 있는 링크를 email로 전송해 주는 예제

## Architecture
![athena_cqrs_pattern_arch](./assets/athena_cqrs_pattern_arch.svg)

## Deployment
1. [Getting Started With the AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)를 참고해서 cdk를 설치하고,
cdk를 실행할 때 사용할 IAM User를 생성한 후, `~/.aws/config`에 등록한다.
예를 들어서, `cdk_user`라는 IAM User를 생성 한 후, 아래와 같이 `~/.aws/config`에 추가로 등록한다.

    ```shell script
    $ cat ~/.aws/config
    [profile cdk_user]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    region=us-east-1
    ```

2. 아래와 같이 소스 코드를 git clone 한 후에, cdk 배포 환경 구성을 한다.

    ```shell script
    $ git clone https://github.com/ksmin23/aws-rss-feed-trans-bot.git
    $ cd aws-rss-feed-trans-bot
    $ python3 -m venv .env
    $ source .env/bin/activate
    (.env) $ pip install -r requirements.txt
    ```

3. `cdk.context.json` 파일을 열어서, `s3_bucket_name_suffix`에 aws athena 쿼리 실행 결과를 저장할 s3 bucket 이름의 suffix를 적고,<br/>
`athena_work_group_name`에 aws athena에서 쿼리를 수행할 Work Group Name을 입력한다.<br/>
`email_from_address`에 e-mail 발신자 주소를 넣는다.<br/>

    ```json
    {
      "vpc_name": "Your-VPC-Name",
      "s3_bucket_name_suffix": "Your-S3-Bucket-Name-Suffix",
      "athena_work_group_name": "Your-Athena-Work-Group-Name",
      "email_from_address": "Your-Sender-Email-Addr"
    }
    ```

   `email_from_address`은 [Amazon SES에서 이메일 주소 확인](https://docs.aws.amazon.com/ses/latest/DeveloperGuide/verify-email-addresses.html)를 참고해서 반드시 사용 가능한 email 주소인지 확인한다. (배포 전에 한번만 확인 하면 된다.)
    예를 들어, `sender@amazon.com`라는 email 주소를 확인하려면 다음과 같이 한다.
      ```
      aws ses verify-email-identity --email-address sender@amazon.com
      ```

4. `cdk deploy` 명령어를 이용해서 배포한다.
    ```shell script
    (.env) $ export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    (.env) $ export CDK_DEFAULT_REGION=us-east-1
    (.env) $ cdk --profile=cdk_user deploy
    ```

5. 배포한 애플리케이션을 삭제하려면, `cdk destroy` 명령어를 아래와 같이 실행 한다.
    ```shell script
    (.env) $ cdk --profile=cdk_user destroy
    ```

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

# Demo
## Send Athena Query

```
$ export API_URL=https://{restapi-id}.execute-api.{region}.amazonaws.com/{stage_name}
$ curl -X POST ${API_URL}/?user=xyz@example.com \
  -H 'Content-Type: application/json' \
  -d'{
    "QueryString": "SELECT dt, impressionid FROM impressions WHERE dt < '2009-04-12-14-00' AND dt >= '2009-04-12-13-00' ORDER BY dt DESC LIMIT 100",
    "QueryExecutionContext": {
      "Database": "hive_ads"
    },
    "ResultConfiguration": {
      "OutputLocation": "s3://bucket-name/path/to/object/"
    }
  }'
```

## Query Excution Results
AWS Athena 쿼리 실행이 완료 되면, e-mail로 쿼리 결과 파일을 다운로드 받을 수 있는 link를 받을 수 있다.

*** Figure 1. *** AWS Athena 쿼리 실행 결과 다운로드 링크 전송 e-mail
![athena-cqrs-pattern-email-screenshot](./assets/athena-cqrs-pattern-email-screenshot.png)
