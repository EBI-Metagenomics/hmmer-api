# from django.contrib import admin
# from unfold.admin import ModelAdmin
# from .models.job import HmmerJob
# from django.utils.html import format_html
# import json


# @admin.register(HmmerJob)
# class HmmerJobAdmin(ModelAdmin):
#     # For the list view
#     list_display = ("id", "algo", "get_task_status", "get_task_date_created")
#     list_select_related = ("task",)

#     # Add filters
#     list_filter = (
#         ("task__date_created", admin.DateFieldListFilter),
#         "task__status",
#         "algo",
#     )

#     # For the detail view
#     fields = ("id", "algo", "get_params_display", "get_task_status", "get_task_result", "get_task_date_created")

#     def get_task_status(self, obj):
#         return obj.task.status if obj.task else "-"

#     get_task_status.short_description = "Status"

#     def get_task_result(self, obj):
#         return obj.task.result if obj.task else "-"

#     get_task_result.short_description = "Result"

#     def get_task_date_created(self, obj):
#         return obj.task.date_created if obj.task else "-"

#     get_task_date_created.short_description = "Created"

#     def get_params_display(self, obj):
#         formatted_json = json.dumps(obj.params, indent=2)
#         return format_html("<pre>{}</pre>", formatted_json)

#     get_params_display.short_description = "Parameters"

#     def get_readonly_fields(self, request, obj=None):
#         return self.fields  # Makes all displayed fields read-only
