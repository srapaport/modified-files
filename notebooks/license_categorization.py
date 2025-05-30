def get_all_license_family(licenses, LICENSE_FAMILIES):
    return {get_license_family(license, LICENSE_FAMILIES) for license in licenses.split(", ")}

def get_license_family(license_name: str, LICENSE_FAMILIES):
    """
    Determine the family of a given license and its position within the family.
    
    Args:
        license_name: Name of the license (lowercase)
        
    Returns:
        Tuple containing (family_name, position_in_family)
        If no family is found, returns ("Unknown", -1)
    """
    license_name = license_name.lower().strip().strip('(').strip(')')
    
    if " with " in license_name:
        base_license = license_name.split(" with ")[0].strip()
        return get_license_family(base_license, LICENSE_FAMILIES)
    
    for family_name, licenses in LICENSE_FAMILIES.items():
        if license_name in licenses:
            return family_name
    
    for family_name, licenses in LICENSE_FAMILIES.items():
        for license_prefix in licenses:
            if license_name.startswith(license_prefix):
                return family_name
    
    return "Unknown"
def classify_license_change(from_family, to_family):

    if "Unknown" in from_family:
        return "Unknown"
    
    if from_family == to_family:
        return "Update"
    
    elif from_family & to_family:
        return "Partial Change"
    
    return "Full Change"