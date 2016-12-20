##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#						    <canma@tencent.com>
#
# Time-stamp: <2010-03-20 10:06:01 macan>
#
# This is the makefile for TDW/PL project.
#
# Armed by EMACS.

HOME_PATH = $(shell pwd)
TDWPL = $(HOME_PATH)/tdwpl
TDWPL_DEPENDS = tdwpl.py tdw_load.py tdw.py
TDWPL_UT_FLAG = -atestshell

all :
	@echo -e " " CD"\t" test
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)"

mc :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" mc

mcheck :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" mcheck

view :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" view

viewcheck :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" viewcheck

check :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" check

clean :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" clean

cleancheck :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" cleancheck

mclean :
	@$(MAKE) --no-print-directory -C test -e "HOME_PATH=$(HOME_PATH)" mclean
