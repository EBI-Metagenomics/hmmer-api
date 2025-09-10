from django.core.validators import BaseValidator
from django.utils.deconstruct import deconstructible
from django.utils.translation import gettext_lazy as _
import logging


@deconstructible
class StrictMaxValueValidator(BaseValidator):
    message = _("Ensure this value is less than %(limit_value)s")
    code = "max_value"

    def compare(self, a, b):
        return a >= b


@deconstructible
class StrictMinValueValidator(BaseValidator):
    message = _("Ensure this value is greater than %(limit_value)s")
    code = "min_value"

    def compare(self, a, b):
        logging.debug("comparing")
        return a <= b
