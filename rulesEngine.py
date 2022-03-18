#!/usr/bin/env python3

import os
from array import array
import json
from nis import match
from shapely.geometry import Polygon, Point #pip3 install shapely
from datetime import datetime


#################
## May require brew install geos on mac, apt-get install libgeos-dev on Linux
#################

observed_areas = []
observed_rules = []

def evaluate(flight):

    matchedRules = []

    for rule in observed_rules:

        if rule['identifier'] in flight['matched_rules']:
            continue

        conditions_met = 0

        for condition in rule['conditions']:

            if condition['type'] == "aircraft_icao_hex":

                if aircraft_icao_hex_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break               

            if condition['type'] == "aircraft_powerplant_count":
                
                if aircraft_powerplant_count_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "aircraft_registration":
                
                if aircraft_registration_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "aircraft_type_designator":
                
                if aircraft_type_designator_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "altitude":
                
                if altitude_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "area":
                
                if area_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "callsign":
                
                if callsign_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "date":
                
                if date_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "heading":
              
                if heading_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "military":
                
                if military_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "operator_airline_designator":
                
                if operator_airline_designator_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break

            if condition['type'] == "squawk":
                
                if squawk_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "velocity":
                
                if velocity_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "vertical_speed":
                
                if vertical_speed_validateData(condition, flight) == True:

                    conditions_met = conditions_met + 1
                else:
                    break   

            if condition['type'] == "wake_turbulence_category":

                if wake_turbulence_category_validateData(condition, flight) == True:
                    
                    conditions_met = conditions_met + 1
                else:
                    break

        if conditions_met == len(rule['conditions']):           
            matchedRules.append(rule)

    return matchedRules


def loadAreas(path):

    global observed_areas

    tmpAreas = []

    if os.path.exists(path) == False:
        raise Exception("File \"" + path + "\" could not be found.")

    with open(path) as geojsonFile:
        geojsonData = json.load(geojsonFile)

    if 'type' not in geojsonData:
        raise Exception("Invalid GeoJSON file, missing 'type'")

    if str(geojsonData['type']).lower() != "featurecollection":
        raise Exception("Invalid GeoJSON file, type must be a featureCollection")

    for feature in geojsonData['features']:

        tmpFeature = {}

        if "type" not in feature:
            raise Exception("Feature " + str(len(tmpAreas)) + " does not contain a \"type\" field.")

        if str(feature['type']).lower() != "feature":
            raise Exception("Feature " + str(len(tmpAreas)) + " \"type\" is invalid; Expected \"Feature\".")

        if "properties" not in feature:
            raise Exception("Feature " + str(len(tmpAreas)) + " does not contain a \"properties\" object.")

        if "name" not in feature['properties']:
            raise Exception("Feature " + str(len(tmpAreas)) + " does not contain a \"name\" field in the \"properties\" object.")

        tmpFeature['name'] = str(feature['properties']['name']).strip()

        if 'geometry' not in feature:
            raise Exception("Feature \"" + tmpFeature['name'] + "\" does not contain a \"geometry\" object.")
        
        if 'type' not in feature['geometry']:
            raise Exception("Feature \"" + tmpFeature['name'] + "\" does not contain a \"type\" field in geometry.")

        if str(feature['geometry']['type']) != "Polygon":
            raise Exception("Feature \"" + tmpFeature['name'] + "\" has an unsupported feature -> geometry -> type of " + feature['geometry']['type'] + "; Only type \"Polygon\" is supported.")

        if "coordinates" not in feature['geometry']:
            raise Exception("Feature \"" + tmpFeature['name'] + "\" does not contain a \"coordinates\" array in geometry.")

        if len(feature['geometry']['coordinates']) != 1:
            raise Exception("Feature \"" + tmpFeature['name'] + "\" does not have an expected number of coordinates in the array.")

        tmpFeature['geometry'] = Polygon([tuple(coord) for coord in feature['geometry']['coordinates'][0]])

        if tmpFeature['geometry'].is_valid == False:
            raise Exception("Feature \"" + tmpFeature['name'] + "\" is not a valid polygon.")

        tmpAreas.append(tmpFeature)

    observed_areas = tmpAreas.copy()


def loadRules(path):

    global observed_rules

    tmpRules = []

    if os.path.exists(path) == False:
        raise Exception("File \"" + path + "\" could not be found.")

    with open(path) as rulesFile:
        rules = json.load(rulesFile)

    if not isinstance(rules, list):
        raise Exception("Rules file does not contain an array of rules.")

    if len(rules) == 0:
        raise Exception("Rules file contains no rules.")

    for rule in rules:

        tmpRule = {}

        if "name" in rule:
            tmpRule['name'] = str(rule['name'])

        if "enabled" not in rule:
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain an \"enabled\" field.")

        if not isinstance(rule['enabled'], bool):
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain a boolean value for \"enabled\".")

        if str(rule['enabled']).lower().strip() != "true":
            continue

        if "description" in rule:
            tmpRule['description'] = str(rule['description'])

        if "identifier" not in rule:
            raise Exception("Rule " + str(len(tmpRules)) +  " does not contain an \"identifier\" field.")

        tmpRule['identifier'] = rule['identifier']

        if "level" not in rule:
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain a \"level\" field.")

        if not isinstance(rule['level'], int):
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain an integer value for \"level\".")

        tmpRule['level'] = rule['level']

        if "conditions" not in rule:
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain a \"conditions\" array.")

        if not isinstance(rule['conditions'], list):
            raise Exception("Rule " + tmpRule['identifier'] +  " does not contain an array of conditions.")

        if len(rule['conditions']) < 1:
            raise Exception("Rule " + tmpRule['identifier'] +  " contains no conditions.")

        tmpConditions = []

        for condition in rule['conditions']:

            if "type" not in condition:
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain a \"type\" field.")

            if not isinstance(condition['type'], str):
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " \"type\" is not a string.")

            if "value" not in condition:
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain a \"value\" field.")

            if "operator" not in condition:
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain an \"operator\" field.")

            if str(condition['type']).lower() not in [
                    'aircraft_icao_hex',
                    'aircraft_powerplant_count',
                    'aircraft_registration',
                    'aircraft_type_designator',
                    'altitude',
                    'area',
                    'callsign',
                    'date',
                    'heading',
                    'military',
                    'operator_airline_designator',
                    'squawk',
                    'velocity',
                    'vertical_speed',
                    'wake_turbulence_category']:
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " contains unknown type \"" + str(condition['type']) + "\".")

            if str(condition['operator']).lower() not in ['equals','minimum','maximum']:
                raise Exception("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " contains unknown operator \"" + str(condition['operator']) + "\".")

            condition['operator'] = str(condition['operator']).lower()
            condition['type'] = str(condition['type']).lower()

            if condition['type'] == "aircraft_icao_hex":
                condition = aircraft_icao_hex_validateCondition(condition)

            if condition['type'] == "aircraft_powerplant_count":
                condition = aircraft_powerplant_count_validateCondition(condition)

            if condition['type'] == "aircraft_registration":
                condition = aircraft_registration_validateCondition(condition)

            if condition['type'] == "aircraft_type_designator":
                condition = aircraft_type_designator_validateCondition(condition)

            if condition['type'] == "altitude":
                condition = altitude_validateCondition(condition)

            if condition['type'] == "area":
                condition = area_validateCondition(condition)

            if condition['type'] == "callsign":
                condition = callsign_validateCondition(condition)

            if condition['type'] == "date":
                condition = date_validateCondition(condition)

            if condition['type'] == "heading":
                condition = heading_validateCondition(condition)

            if condition['type'] == "military":
                condition = military_validateCondition(condition)

            if condition['type'] == "operator_airline_designator":
                condition = operator_airline_designator_validateCondition(condition)

            if condition['type'] == "velocity":
                condition = velocity_validateCondition(condition)

            if condition['type'] == "vertical_speed":
                condition = vertical_speed_validateCondition(condition)

            if condition['type'] == "wake_turbulence_category":
                condition = wake_turbulence_category_validateCondition(condition)
            
            tmpConditions.append(condition)

            tmpRule['conditions'] = tmpConditions.copy()

        tmpRules.append(tmpRule)

    observed_rules = tmpRules.copy()


def aircraft_icao_hex_validateCondition(condition):

    condition['value'] = str(condition['value']).strip().upper()

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    if len(condition['value']) != 6:
        raise exactLengthNotMet(condition, 6)

    return condition


def aircraft_icao_hex_validateData(condition, flight):

    if "icao_hex" not in flight:
        return False

    if str(flight['icao_hex']).strip().lower() == condition['value']:
        return True

    return False


def aircraft_powerplant_count_validateCondition(condition):

    try:
        condition['value'] = int(float(condition['value']))
    except ValueError as ve:
        raise valueNotNumeric(condition)
    
    if condition['value'] < 0:
        raise valueNotPositiveInteger(condition)

    return condition


def aircraft_powerplant_count_validateData(condition, flight):

    if "aircraft" not in flight:
        return False

    if "powerplant" not in flight['aircraft']:
        return False

    if "count" not in flight['aircraft']['powerplant']:
        return False

    if condition['value'] == "equals":

        if flight['aircraft']['powerplant']['count'] ==  condition['value']:
            return True

    if condition['value'] == "minimum":

        if flight['aircraft']['powerplant']['count'] >= condition['value']:
            return True

    if condition['value'] == "maximum":

        if flight['aircraft']['powerplant']['count'] <= condition['value']:
            return True

    return False


def aircraft_registration_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    condition['value'] = str(condition['value']).strip().upper()

    if len(condition['value']) <2:
        raise minimumLengthNotMet(condition, 3)

    return condition


def aircraft_registration_validateData(condition, flight):
    
    if "aircraft" not in flight:
        return False

    if "registration" not in flight['aircraft']:
        return False

    if flight['aircraft']['registration'] == condition['value']:
        return True

    return False
    

def aircraft_type_designator_validateCondition(condition):
    
    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    condition['value'] = str(condition['value']).strip()

    if len(condition['value']) != 4:
        raise exactLengthNotMet(condition, 4)

    return condition


def aircraft_type_designator_validateData(condition, flight):
    
    if "aircraft" not in flight:
        return False

    if "type_designator" not in flight['aircraft']:
        return False

    if flight['aircraft']['type_designator'] == condition['value']:
        return True

    return False
    

def altitude_validateCondition(condition):

    if condition['operator'] == "equals":
        raise operatorShouldNotBeEqualsException(condition)
    
    try:
        condition['value'] = int(float(condition['value']))
    except ValueError:
        raise valueNotNumeric(condition)
    
    if condition['value'] < 0:
        raise valueNotPositiveInteger(condition)

    return condition


def altitude_validateData(condition, flight):
    
    if "positions" not in flight:
        return False

    theLength = len(flight['positions'])
    
    if theLength == 0:
        return False

    if "altitude" not in flight['positions'][theLength-1]:
        return False

    if condition['operator'] == "minimum":
        if flight['positions'][theLength-1]['altitude'] >= condition['value']:
            return True

    if condition['operator'] == "maximum":
        if flight['positions'][theLength-1]['altitude'] <= condition['value']:
            return True
    

def area_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    for area in observed_areas:
        if str(condition['value']).strip().lower() == str(area['name']).lower():
            return condition

    raise Exception(condition['type'] + " \"" + str(condition['value']).strip() + "\" not found in observed areas.")


def area_validateData(condition, flight):
    
    if len(observed_areas) == 0:
        return False

    if "positions" not in flight:
        return False

    theLength = len(flight['positions'])
    
    if theLength == 0:
        return False

    if "latitude" not in flight['positions'][theLength-1]:
        return False

    if "longitude" not in flight['positions'][theLength-1]:
        return False

    point = Point(flight['positions'][theLength-1]['longitude'],flight['positions'][theLength-1]['latitude'])
    
    for area in observed_areas:

        if area['name'] == condition['value']:
            if point.within(area['geometry']) == True:
                return True

    return False


def callsign_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    condition['value'] = str(condition['value']).strip().upper()

    return condition


def callsign_validateData(condition, flight):
    
    if "callsign" not in flight:
        return False

    if flight['callsign'] == condition['value']:
        return True

    return False
    

def date_validateCondition(condition):

    try:
        condition['value'] = datetime.strptime(condition['value'], "%Y-%m-%d").date()

    except ValueError as ve:
        raise Exception(condition['type'] + " \"" + condition['value'] + "\" is a valid date or not in format YYYY-mm-dd.")

    return condition


def date_validateData(condition, flight):

    if condition['operator'] == "equals":
        if datetime.today().utcnow().date() == condition['value']:
            return True

    if condition['operator'] == "minimum":
        if datetime.today().utcnow().date() >= condition['value']:
            return True

    if condition['operator'] == "maximum":
        if datetime.today().utcnow().date() <= condition['value']:
            return True

    return False
    

def heading_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    try:
        condition['value'] = tuple(map(float, condition['value'].split(",")))
    except ValueError:
        raise valueNotNumeric(condition)

    if len(condition['value']) != 2:
        raise Exception(condition['type'] + " \"" + str(condition['value']) + "\" must have exactly two values.")

    for value in condition['value']:
        if value < 0 or value > 359:
            raise Exception(condition['type'] + " \"" + str(value) + "\" is invalid and must be between 0 and 359, inclusive.")

    return condition


def heading_validateData(condition, flight):

    if "velocities" not in flight:
        return False

    theLength = len(flight['velocities'])
    
    if theLength == 0:
        return False

    if "heading" not in flight['velocities'][theLength-1]:
        return False

    if flight['velocities'][theLength-1]['heading'] is None:
        return False

    theHeading = flight['velocities'][theLength-1]['heading']

    #Check for northbound operations, which span 0
    if condition['value'][0] > condition['value'][1]:

        if theHeading >= condition['value'][0] and theHeading <= 359:
            return True

        if theHeading >= 0 and theHeading <=condition['value'][1]:
            return True

    else:
        if theHeading >= condition['value'][0] and theHeading <= condition['value'][1]:
            return True


def military_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    if str(condition['value']).strip().lower() == "true":
        condition['value'] = True
    else:
        condition['value'] = False

    return condition


def military_validateData(condition, flight):

    if "aircraft" not in flight:
        return False

    if "military" not in flight['aircraft']:
        return False

    if flight['aircraft']['military'] == condition['value']:
        return True


def squawk_validateCondition(condition):

    condition['value'] = str(condition['value']).strip()

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    if str(condition['value']).isnumeric() == False:
        raise valueNotNumeric(condition)    

    if len(condition['value']) != 4:
        raise exactLengthNotMet(condition, 4)

    return condition


def squawk_validateData(condition, flight):

    if "squawk" not in flight:
        return False

    if flight['squawk'] == condition['value']:
        return True


def operator_airline_designator_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    condition['value'] = str(condition['value']).strip().upper()

    if len(condition['value']) != 3:
        raise exactLengthNotMet(condition, 3)

    return condition


def operator_airline_designator_validateData(condition, flight):
    return
    

def velocity_validateCondition(condition):

    if condition['operator'] == "equals":
        raise operatorShouldNotBeEqualsException(condition)

    try:
        condition['value'] = int(float(condition['value']))
    except ValueError:
        raise valueNotNumeric(condition)
    
    if condition['value'] < 0:
        raise valueNotPositiveInteger(condition)

    return condition


def velocity_validateData(condition, flight):
    return
    

def vertical_speed_validateCondition(condition):

    if condition['operator'] == "equals":
        raise operatorShouldNotBeEqualsException(condition)

    try:
        condition['value'] = int(float(condition['value']))
    except ValueError:
        raise valueNotNumeric(condition)

    return condition


def vertical_speed_validateData(condition, flight):
    return
    

def wake_turbulence_category_validateCondition(condition):

    if condition['operator'] != "equals":
        raise operatorShouldBeEqualsException(condition)

    condition['value'] = str(condition['value']).strip().lower()

    if condition['value'] not in [
            'light',
            'medium',
            'medium 1',
            'medium 2',
            'high vortex aircraft',
            'heavy',
            'super',
            'rotorcraft',
            'high performance']:
        raise Exception(condition['type'] + " \"" + condition['value'] + "\" is unknown.")

    return condition


def wake_turbulence_category_validateData(condition, flight):

    if "wake_turbulence_category" not in flight['aircraft']:
        return False

    if str(flight['aircraft']['wake_turbulence_category']).strip().lower() == condition['value']:
        return True

    return False


class operatorShouldBeEqualsException(Exception):

    def __init__(self, condition):
        self.message = condition['type'] + " operator must be \"equals\", received \"" + condition['operator'] + "\"."
        super().__init__(self.message)


class operatorShouldNotBeEqualsException(Exception):

    def __init__(self, condition):
        self.message = condition['type'] + " operator must not be \"equals\"."
        super().__init__(self.message)


class valueNotNumeric(Exception):

    def __init__(self, condition):
        self.message = condition['type'] + " value \"" + str(condition['value']).strip() + "\" is not numeric."
        super().__init__(self.message)


class valueNotPositiveInteger(Exception):

    def __init__(self, condition):
        self.message = condition['type'] + " value \"" + str(condition['value']).strip() + "\" is not a positive integer."
        super().__init__(self.message)


class minimumLengthNotMet(Exception):

    def __init__(self, condition, minimum):
        self.message = condition['type'] + " value \"" + str(condition['value']).strip() + "\" must be at least " + str(minimum) + " characters."
        super().__init__(self.message)


class exactLengthNotMet(Exception):

    def __init__(self, condition, minimum):
        self.message = condition['type'] + " value \"" + str(condition['value']).strip() + "\" must be exactly " + str(minimum) + " characters."
        super().__init__(self.message)