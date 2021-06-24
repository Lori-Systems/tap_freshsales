former_version = "{}.freshsales.io"
latest_version = "{}.myfreshworks.com"
NEW_VERSION_EXTENSION = '/crm/sales/'


def check_version(self):
    """
    For the new version API, append the new version extension at the base url
    """
    version = 'old'
    full_domain = self.domain
    if full_domain and len(full_domain.split('.')) == 3:
        subdomain, domain_name, domain = full_domain.split('.')
        if full_domain == latest_version.format(subdomain):
            self.base_url += NEW_VERSION_EXTENSION
            self.version = 'new'
    else:
        raise Exception("Could not check API version!")


def check_credentials(self):
    """
        Make a request to the test endpoint in order to check the user credentials
    """
    test_endpoint = 'api/contacts/filters'
    try:
        response = self.request(method='GET', url=self.url(test_endpoint))
        if response.status_code == 200:
            try:
                response.json()
            except Exception as exp:
                raise Exception("Check credentials! Can not convert response to json.", exp)
    except Exception as e:
        raise Exception("Invalid credentials! ", e)
    return True
