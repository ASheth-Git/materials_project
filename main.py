from mp_api.client import MPRester


class 

# Option 1: Pass your API key directly as an argument.
with MPRester("your_api_key_here") as mpr:
    # do stuff with mpr...

# Option 2: Use the `MP_API_KEY` environment variable:
# export MP_API_KEY="your_api_key_here"
# Note: You can also configure your API key through pymatgen
with MPRester() as mpr:
    # do stuff with mpr ...