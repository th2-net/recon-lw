# Recon-lw

## Table Of Contents

- [Overview](#overview)
- [Module structure](#module-structure)
- [Recon-lw Building Blocks](#recon-lw-building-blocks)
- [Example](#example)

## Overview
This library allows user to:
1. Match message streams by using different rules
2. Process found matches to check if they have any discrepancies
3. Publish events on found discrepencies, draw statistics reports and categorise errors.

## Module structure
### Core
Contains core classes and utility functions that are used in matching/interpretation reconciliation phases.

- recon_lw/core/rule - Defines rule objects which contains everything is needed for recon execution: 
  - collector function/object
  - flush function/object
  - key functions/object
  - matching function/object
- recon_lw/core/type - Defines types used in other entities
- recon_lw/utility - Defines recon_lw core utility functions: 
  - getting stream from file 
  - common state manipulations
  - streams syncing
  - batch extraction

### Interpretation

Contains interfaces, classes and utility functions that are used to interpret matching results.

- recon_lw/interpretation/adapter - [Adapter](#adapter) definition and implementations.
- recon_lw/interpretation/check_rule - [Check rules](#checkrule). Basic field comparison functions/objects.
- recon_lw/interpretation/condition - [Condition](#condition). Common condition functions/objects.
- recon_lw/interpretation/converter - [Converter](#converter). Common field converter functions/objects.
- recon_lw/interpretation/field_checker/ - [Field checker](#fieldchecker). Basic comparison functions.
- recon_lw/interpretation/field_extractor/ - [Field extractor](#extractor). Common functions/objects that allows to extract fields from messages in different ways.
- recon_lw/interpretation/filter/ - [Filter](#extractor). Common filters functions/objects to filter message streams.
- recon_lw/interpretation/interpretation_functions - [Interpretation function](#interpretation-function-provider). Basic functions to interpret matching messages and publish recon events.
  - recon_lw/interpretation/interpretation_functions/event_enhancement - Common event enhancement functions/objects that allows to add additional information to recon events.
  - recon_lw/interpretation/interpretation_functions/event_handling_strategy - Basic event handling strategies ( how to construct recon event on certain condition ) for miss and match events.
  - recon_lw/interpretation/interpretation_functions/event_name_provider - Common event name provider definitions. Describes how to provide name for certain event type.

### Matching

Contains interfaces, classes and utility functions that are used to match message streams.

- recon_lw/matching/collect_matcher - Common [collector functions](#collect-matcher).
- recon_lw/matching/flush_function - Common [flush functions](#flush-function)
- recon_lw/matching/key_functions - Common [key functions](#key-function)
- recon_lw/matching/old - Old collector, flush, matching functions to be backward compatible with older recon versions.
- recon_lw/stream_matcher/ - Common [stream matching functions](#reconmatcher). Different stream matching algorithms.

### Reporting

Defines common report viewers, match and miss error categorisers.

- recon_lw/reporting/coverage/viewer - [Fields coverage report](#coverage). Fields covered by recon.
- recon_lw/reporting/known_issues - [Known issues][#known-issues]. Defines data classes for known issues that are used in categorisation.
- recon_lw/reporting/match_diff 
  - categorizer - Defines common [match diff categorizer](#match-diff-categoriser) which categorises messages that were matched but some fields aren't equal.
  - viewer - Defines common [match diff report viewer](#match-diff-report-viewer) which displays found match diff categories.
- recon_lw/reporting/missing_messages
  -  categorizer - Defines basic [miss_categoriser](#missing-messages-categoriser) which categorises missed messages.
  -  viewer - Defines basic [miss category viewer](#missing-messages-viewer) which shows table after categorisation.
- recon_lw/reporting/recon_context - Defines [recon context](#recon-context) entity that is used in reporting classes to get recon events and to know how to extract fields from messages/events.
- recon_lw/reporting/stats - Defines data classes where recon run statistics is collected.

## Recon-lw Building Blocks

### Entrypoint

Recon execution can be started by calling entrypoint [execute_standalone](recon_lw/recon_lw_entrypoint.py) function:
```python
def execute_standalone(message_pickle_path: Optional[str], sessions_list: Optional[List[str]], result_events_path: Optional[str],
                       rules: Dict[str, Dict[str, Union[Dict[str, Any], AbstractRule]]],
                       data_objects: List[Data]=None,
                       buffer_len=100):
  """
  params:
    :param message_pickle_path - optional path to directory with pickle file that contains th2 messages
    :param rules - a bunch of rules describing how to reconcile multiple streams.
    :param sessions_list - optional list of sessions that should be readed from pickle files
    :param result_events_path - path to directory where recon events should be published.
    :param data_objects - optional list of data objects.
    :param buffer_len - how many messages should be retrieved from data objects list in every iteration. 
  """
...
```

### General execution flow
Common recon execution consist of the following steps:
1. Retrieving batch of messages from data objects
2. Calling [collector function](#collect-matcher) on this batch.
3. Calling [matcher function](#reconmatcher) on this batch to find new matches.
4. Calling [flush matcher](#flush-function) to filter out finalized message matches.
5. Callint [interpretation function](#interpretation-function-provider) to generate recon events on found matches.

### ReconMatcher

- **Interface**: [here](recon_lw/matching/stream_matcher/base.py).
- **Usage**: This entity allows user to define streams matching logic. 

Implementations:
- [OneToMany](recon_lw/matching/stream_matcher/one_many.py)
  - This matcher matches one message from first stream with multiple messages from another stream.
- [PairOne](recon_lw/matching/stream_matcher/pair_one.py)
  - This matcher matches two messages from first stream with one message from second stream.

### Matching key extractor
- **Interface**: [link](recon_lw/matching/matching_key_extractor/base.py)
- **Usage**: This entity defines how key from message should be extracted. Extractors are used in [matchers](#reconmatcher)
to extract key in matchers to use extracted keys for messages matching.

Implementations:
- [SeparatorKeyExtractor](recon_lw/matching/matching_key_extractor/separator.py): this key extractor extracts key from message based on list of fields which combines key and then join values using provided separator.

### Key Function
- **Interface**: [link](recon_lw/matching/key_functions/base.py)
- **Usage**: This entity is used to filter messages to match from stream and to extract key from messages that passed through filter.

Implementations:
- [SimpleOrigKeyFunction](recon_lw/matching/key_functions/simple_original.py)
- [SimpleCopyKeyFunction](recon_lw/matching/key_functions/simple_copy.py)


### Flush function
- **Interface**: [link](recon_lw/matching/flush_function/base.py)
- **Usage**: This entity describes what should be done with found matches between messages streams:

Implementations:
- [DefaultFlushFunction](recon_lw/matching/flush_function/default.py): this flush function collects messages that 
  were matched and passes them into interpretation function which generates events and publish received events.

### Collect Matcher
- **Interface**: [link](recon_lw/matching/collect_macther/base.py)
- **Usage**: This entity describes how and in which order we want to process incoming stream messages batches.

Implementations:
- [DefaultCollectMatcher](recon_lw/matching/collect_matcher/default.py): This implementation just calls [stream 
  matcher](#reconmatcher) implementation delegated to it on every new stream batch.

### Adapter
- **Interface**: [link](recon_lw/interpretation/adapter/base.py)
- **Usage**: The adapter component serves as a crucial intermediary in standardizing data from diverse streams into a uniform format.
Its primary purpose is to facilitate seamless comparison across these streams by abstracting away the need for bespoke
comparison implementations tailored to each stream pairing. This abstraction minimizes redundancy and enhances
efficiency in data processing workflows.

#### Adapter Context
Adapter context is accessible from adapter. Adapter context can be used to get/put some data into cache by other 
building blocks or adapter itself.

#### Implementation
- SimpleAdapter - this is basic adapter. This adapter uses passed by user mapping from `common field name` to 
  [extractor](#extractor) which defines how field should be extracted from message.

- CompoundAdapter - this adapter allows user to use different Adapters for different scenarios. There is multiple 
  [extractors](#extractor) that are chosen based on [conditions](#condition) 

### Extractor

- **Interface**: [link](recon_lw/interpretation/field_extractor/base.py)
- **Usage**: This building block is used in [adapter](#adapter) to define how to get certain field from message.

Implementations:
- [AnyValExtractor](recon_lw/interpretation/field_extractor/any_val.py): returns AnyVal class instance. AnyVal class 
  will return true when compared with any other value except for NOT_EXTRACTED value.
- [SimpleCacheExtractor](recon_lw/interpretation/field_extractor/cache.py): returns field value by field name if 
  exists. If not cache is checked. Cache is filled when field value by field name can be extracted.
- [CacheFillWithConditionExtractor](recon_lw/interpretation/field_extractor/cache.py): updates the cache only if 
  condition is true.
- [ConditionExtractor](recon_lw/interpretation/field_extractor/condition.py): extracts value with one extractor if 
  certain condition is met and uses another extractor if condition isn't met.
- [ConditionMaskExtractor](recon_lw/interpretation/field_extractor/condition.py): masks value if condition is met 
  and returns value if condition isn't met
- [ConstantExtractor](recon_lw/interpretation/field_extractor/constant.py): returns same value every time certain 
  field extraction is issued.
- [NEConstantExtractor](recon_lw/interpretation/field_extractor/constant.py): returns NOT_EXTRACTED value every time 
  certain field extraction is issued.
- [BasicConverterExtractor](recon_lw/interpretation/field_extractor/converter.py): extracts field value with base 
  extractor and then applies converter to it.
- [ChainConverterExtractor](recon_lw/interpretation/field_extractor/converter.py): extracts field value with base 
  extractor and then applies chain of converts to extracted value.
- [BasicDictExtractor](recon_lw/interpretation/field_extractor/dictionary.py): extracts field value by field 
  name from message dictionary without permutations.
- [ListAggregationExtractor](recon_lw/interpretation/field_extractor/list.py): TODO
- [OneOfExtractor](recon_lw/interpretation/field_extractor/one_of.py): extracts values for different field names 
  using different extractors and the first one that is not null is returned to issuing code.
- [SimpleRefDataFieldExtractor](recon_lw/interpretation/field_extractor/refdata.py): extracts field value from 
  message and gets refdata value corresponding to field value.

### Filter
- **Interface**: [link](recon_lw/interpretation/filter/base.py)
- **Usage**: Defines streams filtering rules.

Implementations:
- [SessionAliasFilter](recon_lw/interpretation/filter/session_alias.py) - enables filtering by session alias.
- [MessageTypeFilter](recon_lw/interpretation/filter/message_type.py) - enables filtering by message type.
- [FunctionFilter](recon_lw/interpretation/filter/function.py) - enables filtering by custom user function.
- [FilterChain](recon_lw/interpretation/filter/filter_chain.py) - enables multifilters filtering.
- [FieldFilter](recon_lw/interpretation/filter/field.py) - enables filtering by certain field whitelisted 
  values.
- [DummyFilter](recon_lw/interpretation/filter/dummy.py) - every message isn't filtered.
- [AmendRejectFilter](recon_lw/interpretation/filter/amend_reject.py) - rejects messages with certain reject texts/codes


### FieldChecker
- **Interface**: [link](recon_lw/interpretation/field_checker/base.py)
- **Usage**: Accepts two messages and returns iterator with comparison results comparison being made. This can 
  be used in interpretation function to mark discrepancies.

Implementations:
- [SimpleFieldChecker](recon_lw/interpretation/field_checker/simple.py): Accepts list of fields that needs to be compared and 
  comparison strategy for each field and executes comparison field by field.

### CheckRule
- **Interface**: [link](recon_lw/interpretation/check_rule/base.py)
- **Usage**: Defines comparison logic for field inside messages from different streams.

Implementations:
- [IAdapterFieldCheckRule](recon_lw/interpretation/check_rule/adapter.py): Abstract check rule which uses [adapters](#adapter)
  inside comparison method
- [EqualFieldCheckRule](recon_lw/interpretation/check_rule/equal.py): check rule based on IAdapterFieldCheckRule 
  which extracts field value from messages using adapters and then compares then applying simple equality check.

#### FieldCheckResult
[Data class](recon_lw/interpretation/check_rule/check_result.py) which holds comparison result related informations 
such as: left stream field value, right stream field value, field name, comparison result and comment

### Condition
- **Interface**: [link](recon_lw/interpretation/condition/base.py)
- **Usage**: Helper entity which can be used in other entities implementations. Accepts message and executes some 
  condition under it.

Implementation:
- [FunctionCondition]: Accept user function which executes condition on message.

### Converter
- **Interface**: [link](recon_lw/interpretation/converter/base.py)
- **Usage**: Helper entity which can be used in other entities implementations. Accepts one value and returns 
  another value based on internal logic.

Implementations:
- [BooleanConverter](recon_lw/interpretation/converter/boolean.py): Accepts value and maps it to boolean based on 
  user provided mapping
- [ChainConverter](recon_lw/interpretation/converter/chain.py): Accepts value and applies list of convertes to it 
  one by one.
- [FirstNonNullChainConverter](recon_lw/interpretation/converter/chain.py): Applies converters to value until first 
  non NOT_EXTRACTED value.
- [ConditionConverter](recon_lw/interpretation/converter/condition.py): Applies one converter if condition is met 
  and applies another converter if condition isn't being met.
- [ConstantConverter](recon_lw/interpretation/converter/constant.py): Applies no convertations.
- [DateTimeConverter](recon_lw/interpretation/converter/datetime.py): Takes value and casts it to datetime based on 
  formatting string.
- [DateConverter](recon_lw/interpretation/converter/datetime.py): Takes value and casts it to date based on 
  formatting string.
- [DictPathConverter](recon_lw/interpretation/converter/dictionary.py): Accepts dictionary field value and gets 
  value under specified path inside dictionary.
- [EmptyStringConverter](recon_lw/interpretation/converter/empty_string.py): Returns empty string for each value.
- [FunctionConverter](recon_lw/interpretation/converter/function.py): Accepts user converter function and applies it 
  to value.
- [LengthConverter](recon_lw/interpretation/converter/length.py): Accepts value with length attribute and returns 
  its length.
- [MappingConverter](recon_lw/interpretation/converter/mapping.py): Accepts value and user map and returns mapped 
  value for input value.
- [IndexListConverter](recon_lw/interpretation/converter/list.py): Extracts certain list element based on user index 
  function.
- [RegexConverter](recon_lw/interpretation/converter/regex.py): Applies regex to value and returns matches list.
- [TypeConverter](recon_lw/interpretation/converter/type.py): Casts value to selected type.

### Interpretation Function Provider
- **Interface**: [link](recon_lw/interpretation/interpretation_functions/base.py)
- **Usage**: This entity defines logic for matched messages interpretation.

Implementations:
- [BasicInterpretationFunctionProvider](recon_lw/interpretation/interpretation_functions/simple.py): categorises 
  matched message by four categories: match, match with diff, miss left, miss right. Generates events for different 
  categories using different strategies for match and miss categories. Returns list of events.

#### Event Name Provider
- **Interface**: [link](recon_lw/interpretation/interpretation_functions/event_name_provider/base.py)
- **Usage**: Allows user to define how event names for different match categories should be constructed

Implementations:
- [SimpleMatchEventHandlingStrategy](recon_lw/interpretation/interpretation_functions/event_name_provider/simple.py): defines default event names

#### EventHandlingStrategy
- **Interface**: [link](recon_lw/interpretation/interpretation_functions/event_handling_strategy/base.py)
- **Usage**: Describes how event should be constructed.

Implementations:
- [SimpleMatchEventHandlingStrategy](recon_lw/interpretation/interpretation_functions/event_handling_strategy/base.py): compares fields values using field checker and publishes diff into event. Applies event enhancements.
- [SimpleMissEventHandlingStrategy](recon_lw/interpretation/interpretation_functions/event_handling_strategy/base.py)
  : publishes event with event type ReconEventBasicMissLeft or ReconEventMissRight based on is_copy parameter. 
  Applies event enhancement functions.

### Reporting
These entities are used to give some stats on events gathered during reconcillation.

#### Coverage
- **Implementation**: [link](recon_lw/reporting/coverage/viewer/fields_viewer.py)
- **Usage**: Displays [Recon metadata](recon_lw/reporting/recon_metadata/base.py) object in form of the table where covered fields and descriptions are shown.

#### Known Issues
- **Implementation**: [link](recon_lw/reporting/known_issues/issue.py)
- **Usage**: Data class to store all known issues that were found during previous recon run.

#### Match Diff

##### Match Diff Categoriser
- **Interface**: [link](recon_lw/reporting/match_diff/categorizer/base.py)
- **Usage**: Defines how category names should be extracted from recon events.

Implementations:
- [BasicErrorCategorizer](recon_lw/reporting/match_diff/categorizer/basic.py) - describes simple error categoriser which collects categories basing on [category extractor strategy](recon_lw/reporting/match_diff/categorizer/event_category/base.py) which desribes the way to extract category name for different event types:
  - miss left
  - miss right
  - match
  - match diff

##### Match Diff Report Viewer
- **Implementation**: [link](recon_lw/reporting/match_diff/viewer/category_displayer.py)
- **Usage**: Displays found error categories and examples for them in form of comparison table.

###### Match Diff Color provider

- **Interface**: [link](recon_lw/reporting/match_diff/viewer/color_provider/base.py)
- **Usage**: Allows to define color for different categories types during report displaying.

###### Match Diff Content provider
- **Interface**: [link](recon_lw/reporting/match_diff/viewer/content_provider/base.py)
- **Usage**: Allows to define how content for example should be extracted.

###### Match Diff Style Provider
- **Interface**: [link](recon_lw/reporting/match_diff/viewer/style_provider/base.py)
- **Usage**: Allows to define to provide different css styles for html report table.

Implementations:
- [DefaultStyleProvider](recon_lw/reporting/match_diff/viewer/style_provider/base.py) - defines default styles.

#### Missing Messages
##### Missing Messages Categoriser
- **Interface**: [link](recon_lw/reporting/missing_messages/categorizer/matcher_interface.py)
- **Usage**: Allows to define how to extract category from recon event.

Implementations:
- [SimpleMissesCategorizer](recon_lw/reporting/missing_messages/categorizer/categorizer_impl.py) - defines straightforward categorisation based on [miss category rules](#miss-category-rule)

###### Missing Messages Rule
- **Interface**: [link](recon_lw/reporting/missing_messages/categorizer/rule.py)
- **Usage**: Defines events filter to filter events that falls into issue from rule or not.

##### Missing Messages Viewer
- **Implementation**: [link](recon_lw/reporting/missing_messages/viewer/missing_message.py)
- **Usage**: Defines basic miss categories table html viewer.

## Example

Example featuring main functionality of the library can be found [here](example/example.ipynb) 
