from abc import ABCMeta


class ABCCreateTriggers(ABCMeta, type):
    """
    ABCCreateTriggers is a metaclass that enforces the presence and validation of certain mandatory class attributes
    for any class inheriting from it. This is designed to implement a structure where specific attributes
    must be defined in the derived class with valid values.

    Attributes:
      TABLES_TO_TRACK: Attribute intended for tracking specific tables.
      AUDIT_LOG_CREATE_TABLE: Attribute for audit log creation table.
      AUDIT_LOG_CREATED_CHECK: Attribute to verify if an audit log was created.
      HAS_TRIGGER_CHECK: Attribute for trigger existence verification.
      GET_COLUMN_NAMES: Attribute to get column names.
      INSERT_TRIGGER: Attribute for defining insert triggers.
      UPDATE_TRIGGER: Attribute for defining update triggers.
      DELETE_TRIGGER: Attribute for defining delete triggers.
      _MANDATORY_ATTRIBUTE_MISSING: Internal constant representing a missing attribute.
      _MANDATORY_ATTRIBUTE_UNDEFINED: Internal constant representing an undefined attribute.

    Methods:
      __new__(mcs, name, bases, dct):
        This method is overridden to validate that all mandatory attributes are correctly defined in
        the derived class. If any attributes are missing or have invalid values, a TypeError is raised.

      _valid_value(mcs, base_class, value):
        Class method that checks if a value associated with a mandatory attribute is valid. It ensures
        the attribute is not None and has a non-zero length.

      get_name_value_validation_dict(mcs, bases):
        Class method that builds a name-value validation dictionary by inspecting the base classes.
        This dictionary maps attribute names to their validity status.

      _get_mandatory_class_attrs(mcs):
        Retrieves all mandatory attributes by inspecting the class for uppercase attribute names
        that do not start with an underscore.

      _validate_class_attributes(mcs, mandatory_attrs, name_value_dict):
        Validates the mandatory class attributes. If any of the required attributes are either
        missing or undefined, they are added to the failed validation set. Returns the set of
        attributes that failed validation, along with their validation statuses.
    """
    TABLES_TO_TRACK = None
    AUDIT_LOG_CREATE_TABLE = None
    AUDIT_LOG_CREATED_CHECK = None
    HAS_TRIGGER_CHECK = None
    GET_COLUMN_NAMES = None

    INSERT_TRIGGER = None
    UPDATE_TRIGGER = None
    DELETE_TRIGGER = None

    _MANDATORY_ATTRIBUTE_MISSING = 'missing'
    _MANDATORY_ATTRIBUTE_UNDEFINED = 'undefined'

    def __new__(mcs, name, bases, dct):
        """
        :param mcs: The metaclass instance.
        :param name: The name of the class being created.
        :type name: str
        :param bases: A tuple of the base classes for the class being created.
        :type bases: tuple
        :param dct: A dictionary containing the attributes of the class being created.
        :type dct: dict
        """
        mandatory_class_attrs = mcs._get_mandatory_class_attrs()
        name_value_validation_dict = mcs.get_name_value_validation_dict(bases)

        failed_validation = mcs._validate_class_attributes(mandatory_class_attrs,
                                                           name_value_validation_dict)

        if failed_validation:
            raise TypeError(
                f"{name} is missing the definition for these attributes: {list(failed_validation)}"
            )

        return super().__new__(mcs, name, bases, dct)

    @classmethod
    def _valid_value(mcs, base_class, value):
        """
        :param mcs: The metaclass instance
        :type mcs: type
        :param base_class: The class from which the attribute is being retrieved
        :type base_class: type
        :param value: The name of the attribute to validate
        :type value: str
        :return: A boolean indicating whether the attribute exists, is not None, is not a boolean, and has a non-zero length
        :rtype: bool
        """
        # Retrieve the actual value of the attribute from the base class
        attr_value = getattr(base_class, value, None)
        # Ensure the attribute has a length and is not None (but avoid calling len() on bool or None)
        return (attr_value is not None and not isinstance(attr_value, bool)
                and hasattr(attr_value, '__len__') and len(
            attr_value) > 0)

    @classmethod
    def get_name_value_validation_dict(mcs, bases):
        """
        :param bases: A tuple of base classes to be inspected for attributes that are uppercase and not private (do not start with an underscore).
        :type bases: tuple
        :return: A dictionary mapping attribute names (that are uppercase and not private) to their validated values through the class method `_valid_value`.
        :rtype: dict
        """
        name_value_validation = {}
        for x in bases:
            for y in dir(x):
                if not y.startswith('_') and y.isupper():
                    name_value_validation.update({y: mcs._valid_value(x, y)})
        return name_value_validation

    @classmethod
    def _get_mandatory_class_attrs(mcs):
        """
        Retrieves a list of mandatory class attributes for the metaclass.
        Mandatory attributes are defined as attributes in the class dictionary that:
        1. Do not start with an underscore (_).
        2. Are in uppercase.

        :return: A list of mandatory class attribute names.
        :rtype: list
        """
        return [attr for attr in mcs.__dict__ if not attr.startswith('_') and attr.isupper()]

    @classmethod
    def _validate_class_attributes(mcs, mandatory_attrs, name_value_dict):
        """
        :param mandatory_attrs: A collection containing the attributes that must be present in the `name_value_dict`.
        :type mandatory_attrs: iterable
        :param name_value_dict: A dictionary of attribute names to their respective values to be validated.
        :type name_value_dict: dict
        :return: A set of tuples where each tuple contains a missing or invalid attribute name and its corresponding error type.
        :rtype: set
        """
        failed_validation = {
            (attr,
             mcs._MANDATORY_ATTRIBUTE_UNDEFINED if not value
             else mcs._MANDATORY_ATTRIBUTE_MISSING)
            for attr, value in name_value_dict.items()
            if attr not in mandatory_attrs or not value
        }

        # Add any mandatory attributes that are completely missing from `name_value_dict`
        for attr in mandatory_attrs:
            if attr not in name_value_dict:
                failed_validation.add((attr, mcs._MANDATORY_ATTRIBUTE_MISSING))

        return failed_validation


class ABCPostgresCreateTriggers(ABCCreateTriggers):
    """
    ABCPostgresCreateTriggers is a class that extends ABCCreateTriggers and is designed to handle the creation of triggers in a PostgreSQL database. The class defines attributes related to the logging and validation processes and provides a method to retrieve mandatory class attributes.

    Attributes:
    - LOG_AFTER_INSERT_FUNC: Placeholder for the SQL function name or logic for logging after an insert operation.
    - LOG_AFTER_UPDATE_FUNC: Placeholder for the SQL function name or logic for logging after an update operation.
    - LOG_AFTER_DELETE_FUNC: Placeholder for the SQL function name or logic for logging after a delete operation.
    - FUNC_EXISTS_CHECK: Placeholder for the SQL query or logic to check the existence of a function.
    - VALID_SCHEMA_CHOICES_QUERY: Placeholder for the SQL query to retrieve valid schema choices.

    Methods:
    - _get_mandatory_class_attrs(mcs): Class method to retrieve mandatory class attributes by filtering out private attributes and ensuring they are uppercase.
    """
    LOG_AFTER_INSERT_FUNC = None
    LOG_AFTER_UPDATE_FUNC = None
    LOG_AFTER_DELETE_FUNC = None
    FUNC_EXISTS_CHECK = None
    VALID_SCHEMA_CHOICES_QUERY = None

    @classmethod
    def _get_mandatory_class_attrs(mcs):
        """
        Returns a list of mandatory class attributes defined in the metaclass.
        Mandatory attributes are considered as those which are uppercase and do not start with an underscore.

        :return: List of mandatory class attributes.
        :rtype: list
        """
        return [attr for attr in mcs.__dir__(mcs) if not attr.startswith('_') and attr.isupper()]
