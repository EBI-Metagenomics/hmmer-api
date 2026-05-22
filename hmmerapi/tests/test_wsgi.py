def test_wsgi_application_imports():
    from hmmerapi.wsgi import application

    assert application is not None
