from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("analyze/", views.analyze, name="analyze"),
    path("run-pipeline/", views.run_pipeline, name="run_pipeline"),
    path("data/", views.data_overview, name="data_overview"),
    path("data/<str:symbol>/", views.symbol_detail, name="symbol_detail"),
    path("learn/", views.learn, name="learn"),
    path("about/", views.about, name="about"),
    path("contact/", views.contact, name="contact"),
]
