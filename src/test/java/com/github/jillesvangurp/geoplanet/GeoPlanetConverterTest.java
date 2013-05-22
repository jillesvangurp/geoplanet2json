package com.github.jillesvangurp.geoplanet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.testng.annotations.Test;

@Test
public class GeoPlanetConverterTest {

    public void shouldDequote() {
        assertThat(GeoPlanetConverter.deqoute("\"foo\""), is("foo"));
        assertThat(GeoPlanetConverter.deqoute("foo"), is("foo"));
    }

}
