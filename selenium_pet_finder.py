##############################################################################
# Author            : Amber Zaratsian
# Creation Date     : 24MAY2017
# Description       : Python script to automate web browsing and scraping. 
#                     This will open chrome, navigate to petfinder.com, fill
#                     out the  search filter based on my defined criteria and
#                     scrape the returned results for several elements of 
#                     interest. Finally, this is converted to a Pandas 
#                     DataFrame.
# Python Version    : 2.7
# Operating System  : Mac OS X
# Notes             : This worked on the petfinder.com website in May 2017
##############################################################################

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import time


path_to_chromedriver = '/Users/Beasock/chromedriver' # change path as needed
browser = webdriver.Chrome(executable_path = path_to_chromedriver)


url = 'https://www.petfinder.com/'
browser.get(url)

# Clear location box
browser.find_element_by_id('location').clear()

# Insert location
browser.find_element_by_id('location').send_keys('NC')

# Select animal type from drop down
browser.find_element_by_xpath('//*[@id="fap-type"]/option[text()="Dog"]').click()
# Clear breed box
browser.find_element_by_id('fap-breed-id').clear()

# Insert breed
browser.find_element_by_id('fap-breed-id').send_keys('Dachshund ')

# Click find pets
browser.find_element_by_xpath('//*[@id="find-pets-btn"]').click()

time.sleep(10)
## Results Page ##
# Select distance value from drop down
browser.find_element_by_xpath('//*[@id="fap-distance"]/option[text()="Anywhere"]').click()

# Click box for My Household has other dogs
browser.find_element_by_xpath('//*[@id="fap-household-dogs"]').click()

# Click box for My Household has young childern
browser.find_element_by_xpath('//*[@id="fap-household-children"]').click()

results = browser.find_elements_by_class_name('adoptablePets-item')

d = []
for result in results:
    name   = str(result.find_element_by_class_name('pet-name-container').text).title()
    breed  = str(result.find_element_by_class_name('breed').text)
    specs  = result.find_element_by_class_name('specs').text
    specs  = specs.replace(u' \u2022 ', u',').split(',')
    age    = str(specs[0])
    gender = str(specs[1])
    size   = str(specs[2])
    loc    = str(result.find_element_by_class_name('rescue-info').text).split('\n')[1]
    org    = str(result.find_element_by_class_name('rescue-info').text).split('\n')[0]
    image  = str(result.find_element_by_xpath('//*[@id="search-results"]/li[2]/div/figure/a/img').get_attribute('src'))
    div    = result.find_element_by_class_name('pet-name-container')
    url    = div.find_element_by_css_selector('a').get_attribute('href')
    d.append({"name": name,
              "breed": breed,
              "age": age,
              "gender": gender,
              "size": size,
              "location": loc,
              "shelter": org,
              "image_url": image,
              "link": url})

dogs = pd.DataFrame(d)

dogs.head()

browser.quit()
