<!--
 Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
 for the German Human Genome-Phenome Archive (GHGA)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# Examples

## Stream Calculator
Here we provide an example service called [stream_calc](./stream_calc/stream_calc/).
It accepts simple arithmetic problems as incomming event stream and will send out the
result in another event stream.

### Demo:
We also provide a [small script](./stream_calc/submit_example_problems.py) that submits a couple of
example problems and prints out the results obtained by the stream_calc service.

To run the example, please first start the stream_calc service:
```bash
# (paths relative to this dirctory of this readme)
cd ./stream_calc
python -m stream_calc
```

Now open another terminal (ideally side by side to the old one) and run the client script:
```bash
# (paths relative to this dirctory of this readme)
./stream_calc/submit_example_problems.py
```

### Testing:
The [stream calc test](./stream_calc/sc_tests/) suite demonstrates how to use the
hexkit utilities to test a relying service.

## Auth Demo

This is a REST API that protects several methods in a simple core application
using JSON web tokens for authentication and authorization. Run the API as follows:

```bash
cd auth_demo
pip install -r requirements.txt
python -m auth_demo
```

To check out the API, visit the endpoint http://127.0.0.1:8000/users to get some
tokens for users that can be used with the following other endpoints:

- status: check the login status and expiry date of the token
- reception: show a welcome message to authenticated and non-authenticated users
- lobby: require login as a user and show a welcome message
- lounge: require login as a VIP user and show a different welcome message

To login using the token, you can use the "Authorize" button under
http://127.0.0.1:8000/docs - simply paste on of the bearer tokens here.
Then use the "Try it out" button with the different endpoints. When you're
not logged in, and authentication is required, you will get a "Forbidden" error.
Of course, in a real world application, the user tokens would not be made
freely available, but e.g. derived from an OpenID Connect login.
