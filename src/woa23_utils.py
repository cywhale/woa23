def calculate_sea_density(temperature, salinity, pressure, sigma0=False):
    """
    Calculate sea density based on temperature, salinity, and pressure.

    Parameters:
    - temperature (float): Sea temperature in degrees Celsius.
    - salinity (float): Sea salinity in practical salinity units (PSU).
    - pressure (float): Pressure in decibars (dbar).

    Returns:
    - float: Calculated sea density in kg/m^3.
    """
    import numpy as np

    # Coefficients for density calculation
    a = (
        999.842594
        + 6.793952e-2 * temperature
        - 9.09529e-3 * temperature**2
        + 1.001685e-4 * temperature**3
        - 1.120083e-6 * temperature**4
        + 6.536332e-9 * temperature**5
    )
    b = (
        0.824493
        - 4.0899e-3 * temperature
        + 7.6438e-5 * temperature**2
        - 8.2467e-7 * temperature**3
        + 5.3875e-9 * temperature**4
    )
    c = (
        -5.72466e-3
        + 1.0227e-4 * temperature
        - 1.6546e-6 * temperature**2
    )
    d = 4.8314e-4

    # Density at atmospheric pressure (sigma-t)
    sigma_t = (
        a
        + b * salinity
        + c * salinity**1.5
        + d * salinity**2
    )

    Ahp = (
        3.239908 + 1.43713e-3 * temperature + 1.16092e-4 * temperature**2 - 5.77905e-7 * temperature**3
        + salinity * (2.2838e-3 - 1.0981e-5 * temperature - 1.6078e-6 * temperature**2)
        + salinity**1.5 * (1.91075e-4)
    )

    Bhp = (
        8.50935e-5 - 6.12293e-6 * temperature + 5.2787e-8 * temperature**2
        + salinity * (-9.9348e-7 + 2.0816e-8 * temperature + 9.1697e-10 * temperature**2)
    )

    # Pressure correction
    pressure_correction = (
        19652.21
        + 148.4206 * temperature
        - 2.327105 * temperature**2
        + 1.360477e-2 * temperature**3
        - 5.155288e-5 * temperature**4
        + salinity
        * (54.6746 - 0.603459 * temperature + 1.09987e-2 * temperature**2 - 6.167e-5 * temperature**3)
        + salinity**1.5 * (7.944e-2 + 1.6483e-2 * temperature - 5.3009e-4 * temperature**2)
        + pressure * 0.1 * Ahp
        + (pressure * 0.1)**2 * Bhp
    )

    # Final density with pressure correction
    density = sigma_t / (1 - (pressure * 0.1 / pressure_correction))
    if sigma0:
        density = density - 1000 # potential density anomaly with respect to a reference pressure of 0 dbar (-1000 kg/m^3)

    return density

