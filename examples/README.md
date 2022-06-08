<!--
 Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

Here we provide an example service called [stream_calc](./stream_calc/).
It accepts simple arithmetic problems as incomming event stream and will send out the
result in another event stream.

## Demo:
We also provide a [small script](./submit_example_problems.py) that submits a couple of
example problems and prints out the results obtained by the stream_calc service.

To run the example, please first start the stream_calc service:
```bash
# assuming you are in the directory where the stream_calc package is located:
python -m stream_calc
```

Now open another terminal (ideally side by side to the old one) and run the client script:
```bash
./submit_example_problems.py
```

## Testing:
The [stream_calc_tests](./stream_calc_tests/) suite demonstrates how to use the
hexkit utilities to test a relying service.
