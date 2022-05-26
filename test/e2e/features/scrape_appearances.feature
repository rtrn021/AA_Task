Feature: Scrape Appearences
"""
  Scrape Appearences
  """

  In order to Test quality of Data
  As a Tester
  I want to run Scrape Appearences checks

  @scrape-appearances
  Scenario: Scrape Appearences Test
    Given Initiliase the details for table
    When Read source data for scrape_appearances
    Then Validate the schema
    Then Validate pk check
    Then Validate null check
    Then device column has only supported values
    Then search_term column max char long <= 400
    Then scrape_count column values > 0
    Then date column date format is valid