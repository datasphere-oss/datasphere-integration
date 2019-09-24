package com.datasphere.wizard;

public interface IHazelcastWizard
{
    String validateConnection(final String p0, final String p1, final String p2);
    
    String checkConfigurations(final String p0);
    
    String getAllMapNames(final String p0, final String p1, final String p2);
}
