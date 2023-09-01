#  Copyright 2023 Exactpro (Exactpro Systems Limited)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from th2_data_services.data_source import lwdp
from th2_data_services.config import options
from th2_data_services.utils.converters import flatten_dict


def message_to_dict(message: dict):
    """Converts message body.fields to flatten dict.

    Expects the message after expand_messages function.

    Will return message["simpleBody"] is "simpleBody" in message.
    "simpleBody" -- recon-lw field, that contains flatten dict.

    This function expects the message will be without list in the body.
    So expected message format is:
        {
        ..
        body: {fields: {}, metadata: {}}
        }

    Args:
        message: TH2-Message

    Returns:
        Dict
    """
    # The function was moved from DS-core because it has recon-lw business logic.

    if "simpleBody" in message:
        return message["simpleBody"]

    try:
        result = flatten_dict(options.smfr.get_fields(options.mfr.get_body(message)))
        return result
    except Exception:
        print(
            """message_to_dict function expects the message will be without list in the body.
                So expected message format is:
                    {
                    ..
                    body: {fields: {}, metadata: {}}
                    }
                    """
        )
        print(f"Got the message: {message}")
        raise
