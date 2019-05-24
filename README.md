## Using Ant Colony System to Consolidate VMs for Green Cloud Computing

### Abstract
With the increasing demand for cloud services, the high energy consumption of cloud
data centres is a significant problem that needs to be handled. One way to overcome
this problem is to consolidate the Virtual Machines on the Data centres dynamically.
A VM consolidation approach uses live migration of VMs so that some of the under-
loaded Physical Machines (PMs) can be switched off or put into a low-power mode.
If a Physical Machine (PM) has no consumption at a specified time, it still con-
sumes some energy(static energy). In this project, we further extend this approach
by putting the Physical Machine (PM) to sleep mode, if not in use. We try to imple-
ment the algorithm which will reduce the energy consumption of cloud datacenters
while maintaining the desired Quality of Service(QoS). The VM consolidation is an
NP-Hard problem, so we use a metaheuristic algorithm called Ant Colony System
(ACS). The proposed ACS-based VM Consolidation (ACS-VMC) approach finds a
near-optimal solution based on a specified objective function.

### Tech Stack
- [Java 8](https://www.java.com/)
- [Cloudsim Plus](http://cloudsimplus.org/)

### Submissions
Link to Report: [Report](https://github.com/shivanshs9/acs-vm-migration/blob/master/report.pdf)

Contents:
- Introduction
- Cloud Computing
- Framework and Methodology
- Results
- Conclusion

### Original Paper
- Fahimeh Farahnakian, Adnan Ashraf, Tapio Pahikkala, Pasi Liljeberg, Juha
Plosila, Ivan Porres, and Hannu Tenhunen ”Using Ant Colony System to Consol-
idate VMs for Green Cloud Computing” IEEE TRANSACTIONS ON SERVICES
COMPUTING, Vol. 8, No. 2
