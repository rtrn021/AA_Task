Feature: Competitor Appearences
"""
  Competitor Appearences
  """

  In order to Test quality of Data
  As a Tester
  I want to run Competitor Appearences checks

  @competitor-appearances
  Scenario: Competitor Appearences Test
    Given Initiliase the details for table
    When Read source data for competitor_appearances
    Then Validate the schema
    Then Validate pk check
    Then Validate null check
    Then device column has only supported values
    Then search_term column max char long <= 400
    Then date column date format is valid
    Then domain column max char long <= 100
    Then sponsored_appearances column min value >= 0
    Then natural_appearances column min value >= 0
    Then pla_appearances column min value >= 0
    Then ctr column min value >= 0
    Then ctr column max value <= 1.0
    Then Each row has a corresponding row in scrape_appearances dataset
    Then sponsored_appearances column max value <= scrape_count of scrape_appearances