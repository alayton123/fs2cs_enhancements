# fs2cs_enhancements

22/6/2020

This repository contains code for a python application that enhances the default behaviour of the fs2cs application. 

Initially it reads the old fair scheduler xml and looks for absolute or percetange max values that have been applied to resource pools. It then takes these values, computes the equivalent overall cluster % and updates the corresponding capacity scheduler pool xml with max values (which are set at 100% for every queue by default during the CDP upgrade).

Futher in enhancements are also been consideered.  