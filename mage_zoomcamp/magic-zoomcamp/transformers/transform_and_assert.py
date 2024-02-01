import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
    
def camel_to_snake(name):
    """
    Converts a string from CamelCase to snake_case.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

@transformer
def transform(data, *args, **kwargs):
    print('Rows with zero passagenrs:', data['passenger_count'].isin([0]).sum())
    print('Rows with zero trip_distance:', data['trip_distance'].isin([0]).sum())

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print(data['lpep_pickup_date'].nunique())

    data.columns = [camel_to_snake(col) for col in data.columns]

    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    print(data.shape)
    print(data['vendor_id'].unique())

    return data


@test
def test_output(output, **args):
    assert output['passenger_count'].isin([0]).sum() == 0

@test
def test_output(output, **args):
    assert output['trip_distance'].isin([0]).sum() == 0

@test
def test_output(output, **args):
    assert 'vendor_id' in output.columns