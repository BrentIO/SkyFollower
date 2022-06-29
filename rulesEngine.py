#!/usr/bin/env python3

import os
from array import array
import json
from nis import match
from shapely.geometry import Polygon, Point #pip3 install shapely
from datetime import datetime
import logging


#################
## May require brew install geos on mac, apt-get install libgeos-dev on Linux
#################


class rulesEngine():

    def __init__(self, logger):
        self.logger = logging.getLogger()
        self.logger = logger
        self.observed_areas = []
        self.observed_rules = []
        self.removed_rules = []

    def evaluate(self, flight):

        matchedRules = flight['matched_rules']

        for rule in self.observed_rules:

            if rule['identifier'] in matchedRules:
                continue

            conditions_met = 0

            for condition in rule['conditions']:

                if condition['type'] == "aircraft_icao_hex":

                    if self.aircraft_icao_hex_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break               

                if condition['type'] == "aircraft_powerplant_count":
                    
                    if self.aircraft_powerplant_count_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "aircraft_registration":
                    
                    if self.aircraft_registration_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "aircraft_type_designator":
                    
                    if self.aircraft_type_designator_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "altitude":
                    
                    if self.altitude_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "area":
                    
                    if self.area_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "callsign":
                    
                    if self.callsign_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "date":
                    
                    if self.date_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "heading":
                
                    if self.heading_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "military":
                    
                    if self.military_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "operator_airline_designator":
                    
                    if self.operator_airline_designator_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break

                if condition['type'] == "squawk":
                    
                    if self.squawk_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "velocity":
                    
                    if self.velocity_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "vertical_speed":
                    
                    if self.vertical_speed_validateData(condition, flight) == True:

                        conditions_met = conditions_met + 1
                    else:
                        break   

                if condition['type'] == "wake_turbulence_category":

                    if self.wake_turbulence_category_validateData(condition, flight) == True:
                        
                        conditions_met = conditions_met + 1
                    else:
                        break

            if conditions_met == len(rule['conditions']):           
                matchedRules.append(rule)

        return matchedRules


    def loadAreas(self, path):

        tmpAreas = []

        try:

            if os.path.exists(path) == False:
                raise Exception("Areas file \"" + path + "\" could not be found.")

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
                    self.logger.debug("Feature \"" + tmpFeature['name'] + "\" has an unsupported feature -> geometry -> type of " + feature['geometry']['type'] + "; It will not be imported.")
                    continue

                if "coordinates" not in feature['geometry']:
                    raise Exception("Feature \"" + tmpFeature['name'] + "\" does not contain a \"coordinates\" array in geometry.")

                if len(feature['geometry']['coordinates']) != 1:
                    raise Exception("Feature \"" + tmpFeature['name'] + "\" does not have an expected number of coordinates in the array.")

                tmpFeature['geometry'] = Polygon([tuple(coord) for coord in feature['geometry']['coordinates'][0]])

                if tmpFeature['geometry'].is_valid == False:
                    raise Exception("Feature \"" + tmpFeature['name'] + "\" is not a valid polygon.")

                self.logger.debug("Area \"" + tmpFeature['name'] + "\" has been staged for import.")

                tmpAreas.append(tmpFeature)

            self.observed_areas = tmpAreas

            self.logger.info("All staged areas were imported successfully (" + str(len(self.observed_areas)) + ").")

            return True

        except (self.operatorShouldBeEqualsException, self.operatorShouldNotBeEqualsException,self.valueNotNumeric,
                self.valueNotPositiveInteger, self.minimumLengthNotMet, self.exactLengthNotMet,
                self.ruleCheckException, self.fileNotFound) as ex:
            
            self.logger.critical("Exception while processing areas file.  The file will not be used.  " + ex.message)

            return False

        except json.decoder.JSONDecodeError:
            self.logger.critical("Exception while processing areas file.  Check that the file contains valid JSON.")

        except Exception as ex:
            self.logger.critical("Exception while processing areas file.  The file will not be used.  " + str(ex))

            return False


    def loadRules(self, path):

        tmpRules = []
        tmpRemovedRules = []

        try:

            if os.path.exists(path) == False:
                raise self.fileNotFound("Rules file \"" + path + "\" could not be found.")

            with open(path) as rulesFile:
                rules = json.load(rulesFile)

            if not isinstance(rules, list):
                raise self.ruleCheckException("Rules file does not contain an array of rules.")

            for rule in rules:

                tmpRule = {}

                if "name" in rule:
                    tmpRule['name'] = str(rule['name'])

                if "enabled" not in rule:
                    raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " does not contain an \"enabled\" field.")

                if not isinstance(rule['enabled'], bool):
                    raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " does not contain a boolean value for \"enabled\".")

                if str(rule['enabled']).lower().strip() != "true":
                    continue

                if "description" in rule:
                    tmpRule['description'] = str(rule['description'])

                if "identifier" not in rule:
                    raise self.ruleCheckException("Rule " + str(len(tmpRules)) +  " does not contain an \"identifier\" field.")

                for testRule in tmpRules:
                    if testRule['identifier'] == rule['identifier']:
                        raise self.ruleCheckException("Duplicate rule identifier \"" + rule['identifier'] + "\".")

                if "identifier" not in rule:
                    raise self.ruleCheckException("Rule " + str(len(tmpRules)) +  " does not contain an \"identifier\" field.")

                tmpRule['identifier'] = rule['identifier']

                if "conditions" not in rule:
                    raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " does not contain a \"conditions\" array.")

                if not isinstance(rule['conditions'], list):
                    raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " does not contain an array of conditions.")

                if len(rule['conditions']) < 1:
                    raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " contains no conditions.")

                tmpConditions = []

                for condition in rule['conditions']:

                    if "type" not in condition:
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain a \"type\" field.")

                    if not isinstance(condition['type'], str):
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " \"type\" is not a string.")

                    if "value" not in condition:
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain a \"value\" field.")

                    if "operator" not in condition:
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " does not contain an \"operator\" field.")

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
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " contains unknown type \"" + str(condition['type']) + "\".")

                    if str(condition['operator']).lower() not in ['equals','minimum','maximum']:
                        raise self.ruleCheckException("Rule " + tmpRule['identifier'] +  " condition " + str(len(tmpConditions)) + " contains unknown operator \"" + str(condition['operator']) + "\".")

                    condition['operator'] = str(condition['operator']).lower()
                    condition['type'] = str(condition['type']).lower()

                    if condition['type'] == "aircraft_icao_hex":
                        condition = self.aircraft_icao_hex_validateCondition(condition)

                    if condition['type'] == "aircraft_powerplant_count":
                        condition = self.aircraft_powerplant_count_validateCondition(condition)

                    if condition['type'] == "aircraft_registration":
                        condition = self.aircraft_registration_validateCondition(condition)

                    if condition['type'] == "aircraft_type_designator":
                        condition = self.aircraft_type_designator_validateCondition(condition)

                    if condition['type'] == "altitude":
                        condition = self.altitude_validateCondition(condition)

                    if condition['type'] == "area":
                        condition = self.area_validateCondition(condition)

                    if condition['type'] == "callsign":
                        condition = self.callsign_validateCondition(condition)

                    if condition['type'] == "date":
                        condition = self.date_validateCondition(condition)

                    if condition['type'] == "heading":
                        condition = self.heading_validateCondition(condition)

                    if condition['type'] == "military":
                        condition = self.military_validateCondition(condition)

                    if condition['type'] == "operator_airline_designator":
                        condition = self.operator_airline_designator_validateCondition(condition)

                    if condition['type'] == "squawk":
                        condition = self.squawk_validateCondition(condition)

                    if condition['type'] == "velocity":
                        condition = self.velocity_validateCondition(condition)

                    if condition['type'] == "vertical_speed":
                        condition = self.vertical_speed_validateCondition(condition)

                    if condition['type'] == "wake_turbulence_category":
                        condition = self.wake_turbulence_category_validateCondition(condition)
                    
                    tmpConditions.append(condition)

                    tmpRule['conditions'] = tmpConditions

                tmpRules.append(tmpRule)

            for rule in self.observed_rules:
                if rule not in tmpRules:
                    self.logger.debug("Rule removed: " + rule['name'])
                    tmpRemovedRules.append(rule)

            for rule in tmpRules:
                if rule not in self.observed_rules:
                    self.logger.debug("Rule added: " + rule['name'])

            self.observed_rules = tmpRules
            self.removed_rules = tmpRemovedRules

            self.logger.info("All staged rules were imported successfully (" + str(len(self.observed_rules)) + ").")

            return True

        except (self.operatorShouldBeEqualsException, self.operatorShouldNotBeEqualsException,self.valueNotNumeric,
                self.valueNotPositiveInteger, self.minimumLengthNotMet, self.exactLengthNotMet,
                self.ruleCheckException, self.fileNotFound) as ex:
            
            self.logger.critical("Exception while processing rules file.  The file will not be used.  " + ex.message)

            return False

        except json.decoder.JSONDecodeError:
            self.logger.critical("Exception while processing rules file.  Check that the file contains valid JSON.")

        except Exception as ex:
            self.logger.critical("Exception while processing rules file.  The file will not be used.  " + str(ex))

            return False


    def aircraft_icao_hex_validateCondition(self, condition):

        condition['value'] = str(condition['value']).strip().upper()

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        if len(condition['value']) != 6:
            raise self.exactLengthNotMet(condition, 6)

        return condition


    def aircraft_icao_hex_validateData(self, condition, flight):

        if "icao_hex" not in flight:
            return False

        if str(flight['icao_hex']).strip().lower() == condition['value']:
            return True

        return False


    def aircraft_powerplant_count_validateCondition(self, condition):

        try:
            condition['value'] = int(float(condition['value']))
        except ValueError as ve:
            raise self.valueNotNumeric(condition)
        
        if condition['value'] < 0:
            raise self.valueNotPositiveInteger(condition)

        return condition


    def aircraft_powerplant_count_validateData(self, condition, flight):

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


    def aircraft_registration_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        condition['value'] = str(condition['value']).strip().upper()

        if len(condition['value']) <2:
            raise self.minimumLengthNotMet(condition, 3)

        return condition


    def aircraft_registration_validateData(self, condition, flight):
        
        if "aircraft" not in flight:
            return False

        if "registration" not in flight['aircraft']:
            return False

        if flight['aircraft']['registration'] == condition['value']:
            return True

        return False
        

    def aircraft_type_designator_validateCondition(self, condition):
        
        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        condition['value'] = str(condition['value']).strip()

        if len(condition['value']) != 4:
            raise self.exactLengthNotMet(condition, 4)

        return condition


    def aircraft_type_designator_validateData(self, condition, flight):
        
        if "aircraft" not in flight:
            return False

        if "type_designator" not in flight['aircraft']:
            return False

        if flight['aircraft']['type_designator'] == condition['value']:
            return True

        return False
        

    def altitude_validateCondition(self, condition):

        if condition['operator'] == "equals":
            raise self.operatorShouldNotBeEqualsException(condition)
        
        try:
            condition['value'] = int(float(condition['value']))
        except ValueError:
            raise self.valueNotNumeric(condition)
        
        if condition['value'] < 0:
            raise self.valueNotPositiveInteger(condition)

        return condition


    def altitude_validateData(self, condition, flight):
        
        if "positions" not in flight:
            return False

        theLength = len(flight['positions'])
        
        if theLength == 0:
            return False

        if "altitude" not in flight['positions'][theLength-1]:
            return False

        if not flight['positions'][theLength-1]['altitude']:
            return False

        if condition['operator'] == "minimum":
            if flight['positions'][theLength-1]['altitude'] >= condition['value']:
                return True

        if condition['operator'] == "maximum":
            if flight['positions'][theLength-1]['altitude'] <= condition['value']:
                return True
        

    def area_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        for area in self.observed_areas:
            if str(condition['value']).strip().lower() == str(area['name']).lower():
                return condition

        raise Exception("Area \"" + str(condition['value']).strip() + "\" not found in areas file.")


    def area_validateData(self, condition, flight):
        
        if len(self.observed_areas) == 0:
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
        
        for area in self.observed_areas:

            if area['name'] == condition['value']:
                if point.within(area['geometry']) == True:
                    return True

        return False


    def callsign_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        condition['value'] = str(condition['value']).strip().upper()

        return condition


    def callsign_validateData(self, condition, flight):
        
        if "callsign" not in flight:
            return False

        if flight['callsign'] == condition['value']:
            return True

        return False
        

    def date_validateCondition(self, condition):

        try:
            condition['value'] = datetime.strptime(condition['value'], "%Y-%m-%d").date()

        except ValueError as ve:
            raise Exception(condition['type'] + " \"" + condition['value'] + "\" is a valid date or not in format YYYY-mm-dd.")

        return condition


    def date_validateData(self, condition, flight):

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
        

    def heading_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        try:
            condition['value'] = tuple(map(float, condition['value'].split(",")))
        except ValueError:
            raise self.valueNotNumeric(condition)

        if len(condition['value']) != 2:
            raise Exception(condition['type'] + " \"" + str(condition['value']) + "\" must have exactly two values.")

        for value in condition['value']:
            if value < 0 or value > 359:
                raise Exception(condition['type'] + " \"" + str(value) + "\" is invalid and must be between 0 and 359, inclusive.")

        return condition


    def heading_validateData(self, condition, flight):

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


    def military_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        if str(condition['value']).strip().lower() == "true":
            condition['value'] = True
        else:
            condition['value'] = False

        return condition


    def military_validateData(self, condition, flight):

        if "aircraft" not in flight:
            return False

        if "military" not in flight['aircraft']:
            return False

        if flight['aircraft']['military'] == condition['value']:
            return True


    def squawk_validateCondition(self, condition):

        condition['value'] = str(condition['value']).strip()

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        if str(condition['value']).isnumeric() == False:
            raise self.valueNotNumeric(condition)    

        if len(condition['value']) != 4:
            raise self.exactLengthNotMet(condition, 4)

        return condition


    def squawk_validateData(self, condition, flight):

        if "squawk" not in flight:
            return False

        if flight['squawk'] == condition['value']:
            return True


    def operator_airline_designator_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

        condition['value'] = str(condition['value']).strip().upper()

        if len(condition['value']) != 3:
            raise self.exactLengthNotMet(condition, 3)

        return condition


    def operator_airline_designator_validateData(self, condition, flight):
        
        if "operator" not in flight:
            return False

        if "airline_designator" not in flight['operator']:
            return False

        if flight['operator']['airline_designator'] == condition['value']:
            return True


    def velocity_validateCondition(self, condition):

        if condition['operator'] == "equals":
            raise self.operatorShouldNotBeEqualsException(condition)

        try:
            condition['value'] = int(float(condition['value']))
        except ValueError:
            raise self.valueNotNumeric(condition)
        
        if condition['value'] < 0:
            raise self.valueNotPositiveInteger(condition)

        return condition


    def velocity_validateData(self, condition, flight):
        
        if "velocities" not in flight:
            return False

        theLength = len(flight['velocities'])
        
        if theLength == 0:
            return False

        if "velocity" not in flight['velocities'][theLength-1]:
            return False

        if condition['operator'] == "minimum":
            if flight['velocities'][theLength-1]['velocity'] >= condition['value']:
                return True

        if condition['operator'] == "maximum":
            if flight['velocities'][theLength-1]['velocity'] <= condition['value']:
                return True
        

    def vertical_speed_validateCondition(self, condition):

        if condition['operator'] == "equals":
            raise self.operatorShouldNotBeEqualsException(condition)

        try:
            condition['value'] = int(float(condition['value']))
        except ValueError:
            raise self.valueNotNumeric(condition)

        return condition


    def vertical_speed_validateData(self, condition, flight):
        
        if "velocities" not in flight:
            return False

        theLength = len(flight['velocities'])
        
        if theLength == 0:
            return False

        if "vertical_speed" not in flight['velocities'][theLength-1]:
            return False

        if condition['value'] < 0:
            
            #Condition is Descending
            if condition['operator'] == "minimum":
                if flight['velocities'][theLength-1]['vertical_speed'] < 0 and flight['velocities'][theLength-1]['vertical_speed'] <= condition['value']:
                    return True

            if condition['operator'] == "maximum":
                if flight['velocities'][theLength-1]['vertical_speed'] < 0 and flight['velocities'][theLength-1]['vertical_speed'] >= condition['value']:
                    return True

        else:
            #Condition is Climbing or level
            if condition['operator'] == "minimum":
                if flight['velocities'][theLength-1]['vertical_speed'] >= 0 and flight['velocities'][theLength-1]['vertical_speed'] >= condition['value']:
                    return True

            if condition['operator'] == "maximum":
                if flight['velocities'][theLength-1]['vertical_speed'] >= 0 and flight['velocities'][theLength-1]['vertical_speed'] <= condition['value']:
                    return True


    def wake_turbulence_category_validateCondition(self, condition):

        if condition['operator'] != "equals":
            raise self.operatorShouldBeEqualsException(condition)

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


    def wake_turbulence_category_validateData(self, condition, flight):

        if "wake_turbulence_category" not in flight['aircraft']:
            return False

        if str(flight['aircraft']['wake_turbulence_category']).strip().lower() == condition['value']:
            return True

        return False


    class operatorShouldBeEqualsException(Exception):

        def __init__(self, condition):
            self.message = "Condition type \"" + condition['type'] + "\" operator must be \"equals\", but \"" + condition['operator'] + "\" was specified."
            super().__init__(self.message)


    class operatorShouldNotBeEqualsException(Exception):

        def __init__(self, condition):
            self.message = "Condition type \"" + condition['type'] + "\" operator must not be \"equals\"."
            super().__init__(self.message)


    class valueNotNumeric(Exception):

        def __init__(self, condition):
            self.message = "Condition type \"" + condition['type'] + "\" value \"" + str(condition['value']).strip() + "\" is not numeric."
            super().__init__(self.message)


    class valueNotPositiveInteger(Exception):

        def __init__(self, condition):
            self.message = "Condition type \"" + condition['type'] + "\" value \"" + str(condition['value']).strip() + "\" is not a positive integer."
            super().__init__(self.message)


    class minimumLengthNotMet(Exception):

        def __init__(self, condition, minimum):
            self.message = "Condition type \"" + condition['type'] + "\" value \"" + str(condition['value']).strip() + "\" must be at least " + str(minimum) + " characters."
            super().__init__(self.message)


    class exactLengthNotMet(Exception):

        def __init__(self, condition, minimum):
            self.message = "Condition type \"" + condition['type'] + "\" value \"" + str(condition['value']).strip() + "\" must be exactly " + str(minimum) + " characters."
            super().__init__(self.message)


    class ruleCheckException(Exception):

        def __init__(self, message):
            self.message = message
            super().__init__(self.message)


    class fileNotFound(Exception):

        def __init__(self, message):
            self.message = message
            super().__init__(self.message)
