package dataclean.personalclass;

import dataclean.datatype.BaseEntry;

import java.util.Map;

public class PersonalClass extends BaseEntry {
	public PersonalClass(Map<String, String> args) {
		super(args);
	}

	public PersonalClass() {
		super();
	}

	@Override
	public Boolean check() {
		boolean flag = super.check();
		return flag;
	}
}
